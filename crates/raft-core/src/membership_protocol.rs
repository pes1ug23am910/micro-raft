//! Durable configuration recovery and joint-consensus transitions.
use crate::membership::{
    AdminOperation, AdminRecord, CommittedMembership, ConfigurationEntry, ConfigurationPhase,
    MembershipState, VoterConfig, LEGACY_GROUP_ID,
};
use crate::{
    Command, Effect, Entry, HardState, LogIndex, NodeId, RaftMessage, RaftNode, ReadRejectReason,
    Role, SnapshotDescriptor, Term, CHECK_QUORUM_MS,
};
use std::collections::BTreeSet;

/// Administrative results distinguish admission from durable completion.
#[derive(Clone, Debug, PartialEq)]
pub enum MembershipOutcome {
    Accepted {
        index: LogIndex,
    },
    Recorded {
        record: AdminRecord,
    },
    Rejected {
        reason: String,
        leader_hint: Option<NodeId>,
    },
}

#[derive(Debug)]
pub(crate) struct MembershipRecovery {
    pub committed: CommittedMembership,
    pub effective: MembershipState,
    pub commit_index: LogIndex,
    pub refresh_hard: bool,
}

/// Replay configuration entries without confusing their presence with commitment.
fn replay(
    mut state: CommittedMembership,
    entries: &[Entry],
    through: LogIndex,
) -> Result<CommittedMembership, String> {
    for entry in entries.iter().filter(|entry| entry.index <= through) {
        if let Command::Configuration(change) = &entry.command {
            state = state.advanced(entry.index, entry.term, change)?;
        }
    }
    Ok(state)
}

/// The newest durable configuration entry is effective for voting and quorum
/// decisions immediately, including an uncommitted final entry. Administrative
/// completion remains tied to the separately durable committed watermark.
fn effective_after(
    state: CommittedMembership,
    entries: &[Entry],
    committed: LogIndex,
) -> Result<MembershipState, String> {
    replay(
        state,
        &entries
            .iter()
            .filter(|entry| entry.index > committed)
            .cloned()
            .collect::<Vec<_>>(),
        LogIndex::MAX,
    )
    .map(|state| state.state)
}

/// CURRENT-selected snapshot data and the hard-state membership watermark are
/// independent durable authorities. Reconcile only exact history extensions.
pub(crate) fn recover_membership(
    hard: &HardState,
    image: Option<&SnapshotDescriptor>,
    log: &[Entry],
    legacy_genesis: &[NodeId],
) -> Result<MembershipRecovery, String> {
    let snapshot_index = image.map_or(0, |image| image.metadata.last_included_index);
    let snapshot_membership = image.and_then(|image| image.metadata.membership.as_ref());
    for authority in [hard.membership.as_ref(), snapshot_membership]
        .into_iter()
        .flatten()
    {
        authority.validate()?;
        if authority.term > hard.current_term {
            return Err("membership term exceeds durable term".into());
        }
    }
    let identity = snapshot_membership.or(hard.membership.as_ref());
    let genesis = match identity {
        Some(state) => CommittedMembership::bootstrap_with_group(
            state.state.group_id.clone(),
            state.state.genesis_voters.clone(),
        )?,
        None => CommittedMembership::bootstrap(legacy_genesis.to_vec())?,
    };
    if let Some(image) = image {
        if snapshot_membership.is_none() && image.metadata.members != genesis.state.genesis_voters {
            return Err("legacy snapshot differs from durable genesis".into());
        }
    }
    let base = snapshot_membership.cloned().unwrap_or(genesis);
    let selected = match hard.membership.as_ref() {
        None => base.clone(),
        Some(hard_membership) if base.is_consistent_extension_of(hard_membership) => base.clone(),
        Some(hard_membership) if hard_membership.is_consistent_extension_of(&base) => {
            let last = log.last().map_or(snapshot_index, |entry| entry.index);
            if hard_membership.index > last {
                return Err("membership watermark lacks selected log suffix".into());
            }
            let reconstructed = replay(base.clone(), log, hard_membership.index)?;
            if reconstructed != *hard_membership {
                return Err("membership watermark contradicts selected snapshot or WAL".into());
            }
            hard_membership.clone()
        }
        Some(_) => return Err("durable membership histories contradict".into()),
    };
    let commit_index = snapshot_index.max(selected.index);
    // Every configuration claimed committed by the selected image/metadata
    // must also match any retained log entry at that boundary.
    let verified = replay(base, log, commit_index)?;
    if verified != selected {
        return Err("selected log has unrecorded committed configuration".into());
    }
    let effective = effective_after(selected.clone(), log, commit_index)?;
    let refresh_hard = hard.membership.as_ref() != Some(&selected)
        && (selected.state.group_id != LEGACY_GROUP_ID || selected.index != 0);
    Ok(MembershipRecovery {
        committed: selected,
        effective,
        commit_index,
        refresh_hard,
    })
}

impl RaftNode {
    /// Construct an explicit group independently of the transport address book.
    /// A fresh learner ID may be absent from genesis and stays passive until
    /// replicated admission. Execute the returned durability barrier first.
    pub fn new_for_group(
        id: NodeId,
        genesis_voters: Vec<NodeId>,
        seed: u64,
        group_id: String,
    ) -> Result<(Self, Vec<Effect>), String> {
        // Validate before calling the legacy constructor's uniqueness assertions.
        CommittedMembership::bootstrap_with_group(group_id.clone(), genesis_voters.clone())?;
        let peers = genesis_voters
            .iter()
            .copied()
            .filter(|peer| *peer != id)
            .collect();
        let mut node = Self::new(id, peers, seed);
        let effects = node.initialize_group(group_id, genesis_voters)?;
        Ok((node, effects))
    }

    pub(crate) fn unwrap_group_message(&self, message: RaftMessage) -> Option<RaftMessage> {
        let known = self.committed_membership();
        match message {
            RaftMessage::GroupMessage {
                group_id,
                genesis_voters,
                message,
            } if known.state.group_id != LEGACY_GROUP_ID
                && group_id == known.state.group_id
                && genesis_voters == known.state.genesis_voters
                && !matches!(*message, RaftMessage::GroupMessage { .. }) =>
            {
                Some(*message)
            }
            RaftMessage::GroupMessage { .. } => None,
            message if known.state.group_id == LEGACY_GROUP_ID => Some(message),
            _ => None,
        }
    }

    pub(crate) fn wrap_group_effects(&self, effects: &mut [Effect]) {
        let known = self.committed_membership();
        if known.state.group_id == LEGACY_GROUP_ID {
            return;
        }
        for effect in effects {
            if let Effect::Send { msg, .. } = effect {
                // A callback returns its own complete effect batch; no nested envelopes.
                if matches!(msg, RaftMessage::GroupMessage { .. }) {
                    continue;
                }
                *msg = RaftMessage::GroupMessage {
                    group_id: known.state.group_id.clone(),
                    genesis_voters: known.state.genesis_voters.clone(),
                    message: Box::new(msg.clone()),
                };
            }
        }
    }

    pub(crate) fn on_membership_advertisement(
        &mut self,
        from: NodeId,
        term: Term,
        configuration: CommittedMembership,
        effects: &mut Vec<Effect>,
    ) {
        if from == self.id
            || term < self.hard.current_term
            || configuration.term > term
            || !configuration.state.voters.contains(from)
            || !configuration.is_consistent_extension_of(self.committed_membership())
        {
            return;
        }
        // This is the sender's durable LOG configuration, not a commit proof.
        // It grants correlated replication/election admission only; installed
        // configuration and completed admin records do not change here.
        if let Some(endpoints) = configuration.state.endpoints.get(&from) {
            effects.push(Effect::MembershipRouteHint {
                id: from,
                term,
                endpoints: endpoints.clone(),
            });
        }
        self.membership_advertisements
            .insert(from, (term, configuration));
    }

    pub(crate) fn may_replicate_from(&self, from: NodeId, term: Term) -> bool {
        self.is_voter(from)
            || self
                .membership_advertisements
                .get(&from)
                .is_some_and(|(at, history)| {
                    *at == term && history.is_consistent_extension_of(self.committed_membership())
                })
    }

    pub(crate) fn may_vote_for(
        &self,
        from: NodeId,
        term: Term,
        prospective: bool,
        last_index: LogIndex,
        last_term: Term,
    ) -> bool {
        if self.is_voter(self.id) && self.is_voter(from) {
            return true;
        }
        self.membership_advertisements
            .get(&from)
            .is_some_and(|(at, history)| {
                let expected = if prospective {
                    at.checked_add(1)
                } else {
                    Some(*at)
                };
                expected == Some(term)
                    && history.state.voters.contains(self.id)
                    && history.state.voters.contains(from)
                    && (last_term, last_index) >= (history.term, history.index)
                    && history.is_consistent_extension_of(self.committed_membership())
            })
    }

    pub(crate) fn advertise_membership(&self, to: NodeId, effects: &mut Vec<Effect>) {
        let configuration = self
            .membership_at(self.last_log_index())
            .expect("validated suffix");
        if configuration.state.group_id != LEGACY_GROUP_ID
            && configuration.state.voters.contains(self.id)
        {
            effects.push(Effect::Send {
                to,
                msg: RaftMessage::MembershipAdvertisement {
                    term: self.hard.current_term,
                    configuration,
                },
            });
        }
    }

    pub(crate) fn advertise_replication_membership(
        &mut self,
        to: NodeId,
        effects: &mut Vec<Effect>,
    ) {
        if self.committed_membership().state.group_id == LEGACY_GROUP_ID {
            return;
        }
        let configuration = self
            .membership_at(self.last_log_index())
            .expect("validated suffix");
        let Role::Leader { match_index, .. } = &self.role else {
            return;
        };
        if configuration.index == 0
            || match_index.get(&to).copied().unwrap_or(0) >= configuration.index
        {
            return;
        }
        if self
            .advertisement_last
            .get(&to)
            .is_some_and(|(term, index, at)| {
                *term == self.hard.current_term
                    && *index == configuration.index
                    && self.now_ms.saturating_sub(*at) < CHECK_QUORUM_MS
            })
        {
            return;
        }
        self.advertisement_last.insert(
            to,
            (self.hard.current_term, configuration.index, self.now_ms),
        );
        self.advertise_membership(to, effects);
    }

    pub(crate) fn valid_snapshot_membership(&self, descriptor: &SnapshotDescriptor) -> bool {
        if self.snapshot.as_ref() == Some(descriptor) {
            return true;
        }
        match &descriptor.metadata.membership {
            Some(incoming) => incoming.is_consistent_extension_of(self.committed_membership()),
            None => {
                self.committed_membership().state.group_id == LEGACY_GROUP_ID
                    && descriptor.metadata.members == self.members()
            }
        }
    }

    pub(crate) fn compaction_membership_matches(&self, descriptor: &SnapshotDescriptor) -> bool {
        let Ok(expected) = self.membership_at(descriptor.metadata.last_included_index) else {
            return false;
        };
        match &descriptor.metadata.membership {
            Some(actual) => *actual == expected,
            None => {
                expected.state.group_id == LEGACY_GROUP_ID
                    && descriptor.metadata.members == expected.state.genesis_voters
            }
        }
    }

    pub fn committed_membership(&self) -> &CommittedMembership {
        self.hard
            .membership
            .as_ref()
            .unwrap_or(&self.genesis_membership)
    }

    pub fn effective_membership(&self) -> &MembershipState {
        &self.effective_membership
    }

    pub(crate) fn voter_config(&self) -> &VoterConfig {
        &self.effective_membership.voters
    }

    pub(crate) fn self_quorum(&self) -> bool {
        self.voter_config().has_quorum(&BTreeSet::from([self.id]))
    }

    pub(crate) fn is_voter(&self, id: NodeId) -> bool {
        self.voter_config().contains(id)
    }

    /// Explicit bootstrap/migration barrier. Execute returned persistence before
    /// any tick or listener. Existing groups cannot be silently renamed.
    pub fn initialize_group(
        &mut self,
        group_id: String,
        genesis_voters: Vec<NodeId>,
    ) -> Result<Vec<Effect>, String> {
        let genesis = CommittedMembership::bootstrap_with_group(group_id, genesis_voters)?;
        if genesis.state.group_id == LEGACY_GROUP_ID {
            return Err("explicit group required".into());
        }
        if let Some(existing) = &self.hard.membership {
            if existing.state.group_id == genesis.state.group_id
                && existing.state.genesis_voters == genesis.state.genesis_voters
            {
                return Ok(Vec::new());
            }
            return Err("persisted group identity cannot be changed".into());
        }
        if self.now_ms != 0
            || self.election_deadline_ms != 0
            || !matches!(self.role, Role::Follower)
            || self
                .log
                .iter()
                .any(|entry| matches!(entry.command, Command::Configuration(_)))
            || ((!self.log.is_empty() || self.snapshot.is_some())
                && self.genesis_membership.state.genesis_voters != genesis.state.genesis_voters)
        {
            return Err(
                "group migration requires a pre-service legacy configuration boundary".into(),
            );
        }
        self.genesis_membership = genesis.clone();
        self.hard.membership = Some(genesis.clone());
        self.effective_membership = genesis.state;
        self.update_replication_peers();
        Ok(vec![Effect::PersistHardState(self.hard.clone())])
    }

    /// Recovery must complete as one ordered effect batch before other inputs.
    pub(crate) fn on_recover(&mut self, effects: &mut Vec<Effect>) {
        if !self.recovery_pending {
            return;
        }
        self.recovery_pending = false;
        if self.recovery_refresh_hard {
            effects.push(Effect::PersistHardState(self.hard.clone()));
            self.recovery_refresh_hard = false;
        }
        self.apply_committed(effects);
    }

    pub fn recovery_required(&self) -> bool {
        self.recovery_pending
    }

    pub(crate) fn membership_at(&self, index: LogIndex) -> Result<CommittedMembership, String> {
        if index < self.snapshot_index() {
            return Err("configuration predates retained snapshot".into());
        }
        let base = self
            .snapshot
            .as_ref()
            .and_then(|image| image.metadata.membership.clone())
            .unwrap_or_else(|| self.genesis_membership.clone());
        replay(base, &self.log, index)
    }

    pub(crate) fn update_replication_peers(&mut self) {
        // Retired replicas still receive the durable removal on return. They
        // are never part of any quorum and cannot become voters again.
        self.peers = self
            .effective_membership
            .participants()
            .union(&self.effective_membership.retired)
            .copied()
            .filter(|id| *id != self.id)
            .collect();
        let last = self.last_log_index();
        if let Role::Leader {
            next_index,
            match_index,
        } = &mut self.role
        {
            next_index.retain(|id, _| self.peers.contains(id));
            match_index.retain(|id, _| self.peers.contains(id));
            for peer in &self.peers {
                next_index.entry(*peer).or_insert(last.saturating_add(1));
                match_index.entry(*peer).or_insert(0);
            }
        }
    }

    pub(crate) fn refresh_effective_membership(&mut self, effects: &mut Vec<Effect>) {
        let next = effective_after(
            self.committed_membership().clone(),
            &self.log,
            self.commit_index,
        )
        .expect("validated configuration suffix");
        // A removed leader finishes replicating its final entry under the new
        // quorum, then steps down once that entry is durably committed.
        if self.is_leader()
            && !next.voters.contains(self.id)
            && !self.committed_membership().state.voters.contains(self.id)
        {
            self.become_follower(effects);
        }
        if next == self.effective_membership {
            return;
        }
        let voters_changed = next.voters != self.effective_membership.voters;
        self.effective_membership = next;
        self.update_replication_peers();
        if voters_changed {
            self.reject_pending_reads(ReadRejectReason::LeadershipLost, effects);
            if !self.is_leader() {
                self.become_follower(effects);
            } else if let Some(round) = self.contact_round.checked_add(1) {
                self.contact_round = round;
                self.quorum_contacts = BTreeSet::from([self.id]);
                self.quorum_deadline_ms = self.now_ms.saturating_add(CHECK_QUORUM_MS);
                self.heartbeat_due_ms = self.now_ms;
            } else {
                self.become_follower(effects);
            }
            self.reset_election_deadline();
        }
    }

    pub(crate) fn advance_commit(&mut self, index: LogIndex, effects: &mut Vec<Effect>) {
        if index <= self.commit_index {
            return;
        }
        let next = self
            .membership_at(index)
            .expect("validated committed configuration");
        let changed = next != *self.committed_membership();
        if changed {
            self.hard.membership = Some(next.clone());
            effects.push(Effect::PersistHardState(self.hard.clone()));
        }
        self.commit_index = index;
        self.refresh_effective_membership(effects);
        self.apply_committed(effects);
        if changed {
            effects.push(Effect::MembershipChanged { committed: next });
        }
    }

    pub(crate) fn quorum_commit_candidate(&mut self) -> Option<LogIndex> {
        let Role::Leader { match_index, .. } = &self.role else {
            return None;
        };
        let mut matched = match_index.clone();
        matched.insert(self.id, self.last_log_index());
        let quorum_index = self.voter_config().replicated_index(&matched);
        #[cfg(test)]
        let quorum_index = if self.membership_union_fault
            && matches!(self.voter_config(), VoterConfig::Joint { .. })
        {
            self.membership_fault_hits += 1;
            VoterConfig::Stable {
                voters: self.voter_config().voters().into_iter().collect(),
            }
            .replicated_index(&matched)
        } else {
            quorum_index
        };
        self.log
            .iter()
            .rev()
            .find(|entry| {
                entry.index > self.commit_index
                    && entry.index <= quorum_index
                    && entry.term == self.hard.current_term
            })
            .map(|entry| entry.index)
    }

    pub(crate) fn valid_configuration_suffix(&self, entries: &[Entry]) -> bool {
        let base = self
            .snapshot
            .as_ref()
            .and_then(|image| image.metadata.membership.clone())
            .unwrap_or_else(|| self.genesis_membership.clone());
        replay(base, entries, LogIndex::MAX).is_ok()
    }

    pub(crate) fn on_membership_change(
        &mut self,
        request_id: String,
        operation: AdminOperation,
        effects: &mut Vec<Effect>,
    ) {
        let outcome = self.admit_membership(&request_id, operation, effects);
        effects.push(Effect::MembershipResult {
            request_id,
            outcome,
        });
    }

    fn reject_admin(&self, reason: impl Into<String>) -> MembershipOutcome {
        MembershipOutcome::Rejected {
            reason: reason.into(),
            leader_hint: self.leader_hint,
        }
    }

    fn admit_membership(
        &mut self,
        request_id: &str,
        operation: AdminOperation,
        effects: &mut Vec<Effect>,
    ) -> MembershipOutcome {
        if !self.is_leader() {
            return self.reject_admin("not_leader");
        }
        if let Some(record) = self.committed_membership().state.records.get(request_id) {
            return if record.operation == operation {
                MembershipOutcome::Recorded {
                    record: record.clone(),
                }
            } else {
                self.reject_admin("request_payload_changed")
            };
        }
        if let Some(entry) = self.log.iter().find(|entry| {
            matches!(&entry.command,
            Command::Configuration(change) if change.request_id == request_id)
        }) {
            let Command::Configuration(change) = &entry.command else {
                unreachable!()
            };
            return if change.operation == operation {
                MembershipOutcome::Accepted { index: entry.index }
            } else {
                self.reject_admin("request_payload_changed")
            };
        }
        if self.log.iter().any(|entry| {
            entry.index > self.commit_index && matches!(entry.command, Command::Configuration(_))
        }) {
            return self.reject_admin("membership_change_pending");
        }
        if self.commit_index == 0
            || self.log_term(self.commit_index) != Some(self.hard.current_term)
        {
            return self.reject_admin("current_term_not_committed");
        }
        let phase = match self
            .committed_membership()
            .state
            .first_phase(request_id, &operation)
        {
            Ok(phase) => phase,
            Err(reason) => return self.reject_admin(reason),
        };
        if let AdminOperation::SetVoters { voters } = &operation {
            let fence = match &self.promotion_fence {
                Some((id, prior, fence)) if id == request_id && prior == &operation => *fence,
                Some((id, _, _)) if id == request_id => {
                    return self.reject_admin("request_payload_changed")
                }
                _ => {
                    let fence = self.last_log_index();
                    self.promotion_fence = Some((request_id.to_owned(), operation.clone(), fence));
                    fence
                }
            };
            let Role::Leader { match_index, .. } = &self.role else {
                unreachable!()
            };
            if voters.iter().any(|id| {
                !self.is_voter(*id)
                    && (match_index.get(id).copied().unwrap_or(0) < fence
                        || !self.quorum_contacts.contains(id))
            }) {
                return self.reject_admin("learner_not_caught_up");
            }
        }
        let change = ConfigurationEntry {
            request_id: request_id.to_owned(),
            operation,
            phase,
        };
        match self.append_configuration(change, effects) {
            Some(index) => {
                self.promotion_fence = None;
                MembershipOutcome::Accepted { index }
            }
            None => self.reject_admin("log_index_exhausted"),
        }
    }

    fn append_configuration(
        &mut self,
        change: ConfigurationEntry,
        effects: &mut Vec<Effect>,
    ) -> Option<LogIndex> {
        let index = self
            .last_log_index()
            .checked_add(1)
            .filter(|index| *index < LogIndex::MAX)?;
        let entry = Entry {
            index,
            term: self.hard.current_term,
            command: Command::Configuration(change),
        };
        self.log.push(entry.clone());
        effects.push(Effect::PersistLogEntries {
            truncate_from: None,
            entries: vec![entry],
        });
        self.refresh_effective_membership(effects);
        if let Some(committed) = self.quorum_commit_candidate() {
            self.advance_commit(committed, effects);
        }
        Some(index)
    }

    pub(crate) fn maybe_finalize_membership(&mut self, effects: &mut Vec<Effect>) {
        if !self.is_leader()
            || self.log.iter().any(|entry| {
                entry.index > self.commit_index
                    && matches!(entry.command, Command::Configuration(_))
            })
        {
            return;
        }
        let Some((id, record)) = self.committed_membership().state.pending() else {
            return;
        };
        let change = ConfigurationEntry {
            request_id: id.to_owned(),
            operation: record.operation.clone(),
            phase: ConfigurationPhase::Final,
        };
        self.append_configuration(change, effects);
    }
}
