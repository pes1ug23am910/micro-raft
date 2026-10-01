//! Quorum arithmetic for the membership extension.
//!
//! RaftNode activates this validated state through membership_protocol.
//! Replication participants and learners must never be counted merely because
//! they are reachable; only the voters in this configuration carry authority.

use crate::{LogIndex, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum VoterConfig {
    Stable { voters: Vec<NodeId> },
    Joint { old: Vec<NodeId>, new: Vec<NodeId> },
}

fn validate_set(voters: &[NodeId]) -> Result<(), String> {
    if voters.is_empty() || voters.len() > 256 || voters.windows(2).any(|pair| pair[0] >= pair[1]) {
        Err("voters must be nonempty, sorted and unique".into())
    } else {
        Ok(())
    }
}

fn majority(voters: &[NodeId], votes: &BTreeSet<NodeId>) -> bool {
    voters.iter().filter(|id| votes.contains(id)).count() > voters.len() / 2
}

fn majority_index(voters: &[NodeId], matched: &BTreeMap<NodeId, LogIndex>) -> LogIndex {
    if voters.is_empty() {
        return 0;
    }
    let mut indexes: Vec<_> = voters
        .iter()
        .map(|id| matched.get(id).copied().unwrap_or(0))
        .collect();
    indexes.sort_unstable();
    indexes[indexes.len() - (indexes.len() / 2 + 1)]
}

impl VoterConfig {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Stable { voters } => validate_set(voters),
            Self::Joint { old, new } => {
                validate_set(old)?;
                validate_set(new)
            }
        }
    }

    /// Invalid configurations fail closed even when a caller skipped validation.
    pub fn has_quorum(&self, votes: &BTreeSet<NodeId>) -> bool {
        if self.validate().is_err() {
            return false;
        }
        match self {
            Self::Stable { voters } => majority(voters, votes),
            Self::Joint { old, new } => majority(old, votes) && majority(new, votes),
        }
    }

    /// Highest index replicated to the required quorum. This is only a count;
    /// the caller must separately enforce current-term and config-boundary rules.
    pub fn replicated_index(&self, matched: &BTreeMap<NodeId, LogIndex>) -> LogIndex {
        if self.validate().is_err() {
            return 0;
        }
        match self {
            Self::Stable { voters } => majority_index(voters, matched),
            Self::Joint { old, new } => {
                majority_index(old, matched).min(majority_index(new, matched))
            }
        }
    }

    pub fn voters(&self) -> BTreeSet<NodeId> {
        match self {
            Self::Stable { voters } => voters.iter().copied().collect(),
            Self::Joint { old, new } => old.iter().chain(new).copied().collect(),
        }
    }

    pub fn contains(&self, id: NodeId) -> bool {
        match self {
            Self::Stable { voters } => voters.contains(&id),
            Self::Joint { old, new } => old.contains(&id) || new.contains(&id),
        }
    }
}

/// Administrative identities are retained; reaching the cap is visible refusal.
pub const MAX_ADMIN_RECORDS: usize = 1024;
/// Conservative JSON bound for all validated membership fields together.
/// Advertisements are separate frames; snapshot chunks carry one such history.
pub const MAX_MEMBERSHIP_JSON_BYTES: usize = 3 * 1024 * 1024;
pub const LEGACY_GROUP_ID: &str = "legacy-default";
pub const MAX_ADMIN_ID_BYTES: usize = 128;
pub const MAX_MEMBER_ENDPOINT_BYTES: usize = 512;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberEndpoints {
    pub raft: String,
    pub http: String,
}

impl MemberEndpoints {
    pub fn validate(&self) -> Result<(), String> {
        for endpoint in [&self.raft, &self.http] {
            if endpoint.is_empty()
                || endpoint.len() > MAX_MEMBER_ENDPOINT_BYTES
                || endpoint.trim() != endpoint
                || endpoint.chars().any(char::is_control)
            {
                return Err("invalid bounded member endpoint".into());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdminOperation {
    AddLearner {
        id: NodeId,
        endpoints: MemberEndpoints,
    },
    Remove {
        id: NodeId,
    },
    SetVoters {
        voters: Vec<NodeId>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationPhase {
    Apply,
    Joint,
    Final,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationEntry {
    pub request_id: String,
    pub operation: AdminOperation,
    pub phase: ConfigurationPhase,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRecord {
    pub operation: AdminOperation,
    pub first_index: LogIndex,
    pub first_term: crate::Term,
    pub joint: bool,
    pub final_index: Option<LogIndex>,
    pub final_term: Option<crate::Term>,
}

/// Configuration and exact administrative outcomes selected by durable metadata.
/// Replica IDs have one lifetime; retired identities cannot silently rejoin.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipState {
    pub version: u32,
    pub group_id: String,
    pub genesis_voters: Vec<NodeId>,
    pub voters: VoterConfig,
    pub learners: BTreeSet<NodeId>,
    pub retired: BTreeSet<NodeId>,
    pub endpoints: BTreeMap<NodeId, MemberEndpoints>,
    pub records: BTreeMap<String, AdminRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedMembership {
    pub index: LogIndex,
    pub term: crate::Term,
    pub state: MembershipState,
}

impl MembershipState {
    pub fn bootstrap(voters: Vec<NodeId>) -> Result<Self, String> {
        Self::bootstrap_with_group(LEGACY_GROUP_ID.to_owned(), voters)
    }

    pub fn bootstrap_with_group(group_id: String, voters: Vec<NodeId>) -> Result<Self, String> {
        Self::validate_request_id(&group_id)?;
        validate_set(&voters)?;
        Ok(Self {
            version: 1,
            group_id,
            genesis_voters: voters.clone(),
            voters: VoterConfig::Stable { voters },
            learners: BTreeSet::new(),
            retired: BTreeSet::new(),
            endpoints: BTreeMap::new(),
            records: BTreeMap::new(),
        })
    }

    pub fn participants(&self) -> BTreeSet<NodeId> {
        self.voters
            .voters()
            .union(&self.learners)
            .copied()
            .collect()
    }

    pub fn pending(&self) -> Option<(&str, &AdminRecord)> {
        self.records
            .iter()
            .find(|(_, record)| record.final_index.is_none())
            .map(|(id, record)| (id.as_str(), record))
    }

    fn validate_request_id(id: &str) -> Result<(), String> {
        if id.is_empty()
            || id.len() > MAX_ADMIN_ID_BYTES
            || !id.is_ascii()
            || id
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        {
            Err(
                "administrative request ID must be bounded printable ASCII without whitespace"
                    .into(),
            )
        } else {
            Ok(())
        }
    }

    /// Determine a change's first protocol phase without granting admission.
    /// The core separately checks current leadership and learner catch-up proof.
    pub fn first_phase(
        &self,
        id: &str,
        operation: &AdminOperation,
    ) -> Result<ConfigurationPhase, String> {
        Self::validate_request_id(id)?;
        if self.group_id == LEGACY_GROUP_ID {
            return Err("explicit_group_identity_required".into());
        }
        if let Some(record) = self.records.get(id) {
            return if record.operation == *operation {
                Err("request_already_recorded".into())
            } else {
                Err("request_payload_changed".into())
            };
        }
        if self.pending().is_some() || matches!(self.voters, VoterConfig::Joint { .. }) {
            return Err("membership_change_pending".into());
        }
        if self.records.len() >= MAX_ADMIN_RECORDS {
            return Err("membership_admin_capacity".into());
        }
        match operation {
            AdminOperation::AddLearner { id, endpoints } => {
                endpoints.validate()?;
                if self.retired.contains(id) {
                    return Err("member_id_retired".into());
                }
                if self.participants().contains(id) {
                    return Err("member_already_admitted".into());
                }
                Ok(ConfigurationPhase::Apply)
            }
            AdminOperation::Remove { id } if self.learners.contains(id) => {
                Ok(ConfigurationPhase::Apply)
            }
            AdminOperation::Remove { id } => {
                let voters = self.voters.voters();
                if !voters.contains(id) {
                    return Err("member_unknown".into());
                }
                if voters.len() == 1 {
                    return Err("cannot_remove_last_voter".into());
                }
                Ok(ConfigurationPhase::Joint)
            }
            AdminOperation::SetVoters { voters } => {
                validate_set(voters)?;
                let current = self.voters.voters();
                let requested: BTreeSet<_> = voters.iter().copied().collect();
                if current == requested {
                    return Err("voters_unchanged".into());
                }
                if requested
                    .iter()
                    .any(|id| !current.contains(id) && !self.learners.contains(id))
                {
                    return Err("new_voter_must_be_admitted_learner".into());
                }
                Ok(ConfigurationPhase::Joint)
            }
        }
    }

    fn target_voters(&self, operation: &AdminOperation) -> Result<Vec<NodeId>, String> {
        match operation {
            AdminOperation::SetVoters { voters } => Ok(voters.clone()),
            AdminOperation::Remove { id } => Ok(self
                .voters
                .voters()
                .into_iter()
                .filter(|voter| voter != id)
                .collect()),
            AdminOperation::AddLearner { .. } => {
                Err("learner admission does not change voter quorum".into())
            }
        }
    }

    /// Replay one structurally valid committed configuration log entry. This
    /// function performs no quorum decision and must not imply one was obtained.
    fn apply_entry(
        &mut self,
        index: LogIndex,
        term: crate::Term,
        entry: &ConfigurationEntry,
    ) -> Result<(), String> {
        if index == 0 || index == LogIndex::MAX || term == 0 {
            return Err("invalid configuration log boundary".into());
        }
        Self::validate_request_id(&entry.request_id)?;
        if entry.phase == ConfigurationPhase::Final {
            let (pending_id, record) = self.pending().ok_or("no joint operation to finish")?;
            if pending_id != entry.request_id
                || record.operation != entry.operation
                || index <= record.first_index
                || term < record.first_term
            {
                return Err("final entry differs from active joint operation".into());
            }
            let VoterConfig::Joint { old, new } = &self.voters else {
                return Err("final entry requires joint voters".into());
            };
            let new = new.clone();
            let outgoing: Vec<_> = old.iter().filter(|id| !new.contains(id)).copied().collect();
            self.retired.extend(outgoing);
            self.voters = VoterConfig::Stable { voters: new };
            let record = self
                .records
                .get_mut(&entry.request_id)
                .expect("pending record");
            record.final_index = Some(index);
            record.final_term = Some(term);
            return Ok(());
        }
        let expected = self.first_phase(&entry.request_id, &entry.operation)?;
        if entry.phase != expected {
            return Err("incorrect first configuration phase".into());
        }
        let joint = entry.phase == ConfigurationPhase::Joint;
        if joint {
            let old = self.voters.voters().into_iter().collect();
            let new = self.target_voters(&entry.operation)?;
            for id in &new {
                self.learners.remove(id);
            }
            self.voters = VoterConfig::Joint { old, new };
        } else {
            match &entry.operation {
                AdminOperation::AddLearner { id, endpoints } => {
                    self.learners.insert(*id);
                    self.endpoints.insert(*id, endpoints.clone());
                }
                AdminOperation::Remove { id } => {
                    self.learners.remove(id);
                    self.retired.insert(*id);
                }
                AdminOperation::SetVoters { .. } => {
                    return Err("voter change cannot use single-phase apply".into())
                }
            }
        }
        self.records.insert(
            entry.request_id.clone(),
            AdminRecord {
                operation: entry.operation.clone(),
                first_index: index,
                first_term: term,
                joint,
                final_index: (!joint).then_some(index),
                final_term: (!joint).then_some(term),
            },
        );
        Ok(())
    }
}

impl CommittedMembership {
    pub fn bootstrap(voters: Vec<NodeId>) -> Result<Self, String> {
        Ok(Self {
            index: 0,
            term: 0,
            state: MembershipState::bootstrap(voters)?,
        })
    }

    pub fn bootstrap_with_group(group_id: String, voters: Vec<NodeId>) -> Result<Self, String> {
        Ok(Self {
            index: 0,
            term: 0,
            state: MembershipState::bootstrap_with_group(group_id, voters)?,
        })
    }

    /// A newer advertised history may complete an older pending joint request,
    /// but it cannot change any fact already durably known by this receiver.
    pub fn is_consistent_extension_of(&self, older: &Self) -> bool {
        if self.validate().is_err()
            || older.validate().is_err()
            || self.state.group_id != older.state.group_id
            || self.state.genesis_voters != older.state.genesis_voters
            || self.index < older.index
            || self.term < older.term
        {
            return false;
        }
        for (id, old) in &older.state.records {
            let Some(new) = self.state.records.get(id) else {
                return false;
            };
            if new.operation != old.operation
                || new.first_index != old.first_index
                || new.first_term != old.first_term
                || new.joint != old.joint
            {
                return false;
            }
            if old.final_index.is_some()
                && (new.final_index != old.final_index || new.final_term != old.final_term)
            {
                return false;
            }
            // A completion absent from an older committed prefix must occur
            // strictly after that prefix, not be backfilled into its past.
            if old.final_index.is_none()
                && new.final_index.is_some_and(|index| index <= older.index)
            {
                return false;
            }
        }
        for (id, new) in &self.state.records {
            if !older.state.records.contains_key(id) && new.first_index <= older.index {
                return false;
            }
        }
        true
    }

    pub fn advanced(
        &self,
        index: LogIndex,
        term: crate::Term,
        entry: &ConfigurationEntry,
    ) -> Result<Self, String> {
        if index <= self.index || term < self.term {
            return Err("configuration history regressed".into());
        }
        let mut result = self.clone();
        result.state.apply_entry(index, term, entry)?;
        result.index = index;
        result.term = term;
        Ok(result)
    }

    /// Reconstruct the entire retained administrative history and compare it
    /// with the claimed state. A snapshot cannot invent a voter/retired set by
    /// editing its final arrays while retaining contradictory history.
    pub fn validate(&self) -> Result<(), String> {
        if self.state.version != 1 || self.state.records.len() > MAX_ADMIN_RECORDS {
            return Err("unsupported or oversized membership state".into());
        }
        let mut rebuilt = Self::bootstrap_with_group(
            self.state.group_id.clone(),
            self.state.genesis_voters.clone(),
        )?;
        let mut events = Vec::new();
        for (id, record) in &self.state.records {
            let phase = if record.joint {
                ConfigurationPhase::Joint
            } else {
                ConfigurationPhase::Apply
            };
            events.push((
                record.first_index,
                record.first_term,
                ConfigurationEntry {
                    request_id: id.clone(),
                    operation: record.operation.clone(),
                    phase,
                },
            ));
            match (record.final_index, record.final_term) {
                (Some(index), Some(term)) if record.joint => events.push((
                    index,
                    term,
                    ConfigurationEntry {
                        request_id: id.clone(),
                        operation: record.operation.clone(),
                        phase: ConfigurationPhase::Final,
                    },
                )),
                (Some(index), Some(term))
                    if index == record.first_index && term == record.first_term => {}
                (None, None) if record.joint => {}
                _ => return Err("inconsistent administrative completion boundary".into()),
            }
        }
        events.sort_by_key(|event| event.0);
        for (index, term, entry) in events {
            rebuilt = rebuilt.advanced(index, term, &entry)?;
        }
        if &rebuilt != self {
            return Err("membership state differs from its retained history".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn initial() -> CommittedMembership {
        CommittedMembership::bootstrap_with_group("group-a".into(), vec![1, 2, 3]).unwrap()
    }
    fn learner(id: NodeId) -> AdminOperation {
        AdminOperation::AddLearner {
            id,
            endpoints: MemberEndpoints {
                raft: format!("node-{id}:7000"),
                http: format!("node-{id}:8000"),
            },
        }
    }
    fn change(
        request: &str,
        operation: AdminOperation,
        phase: ConfigurationPhase,
    ) -> ConfigurationEntry {
        ConfigurationEntry {
            request_id: request.into(),
            operation,
            phase,
        }
    }

    #[test]
    fn learner_joint_final_history_retains_exact_results_and_retired_identity() {
        let one = initial()
            .advanced(
                10,
                3,
                &change("add-4", learner(4), ConfigurationPhase::Apply),
            )
            .unwrap();
        assert!(one.state.learners.contains(&4));
        assert!(!one.state.voters.contains(4));
        let operation = AdminOperation::SetVoters {
            voters: vec![2, 3, 4],
        };
        let joint = one
            .advanced(
                20,
                3,
                &change("replace-1", operation.clone(), ConfigurationPhase::Joint),
            )
            .unwrap();
        assert!(
            matches!(&joint.state.voters,VoterConfig::Joint{old,new}if old==&vec![1,2,3]&&new==&vec![2,3,4])
        );
        assert!(!joint.state.learners.contains(&4));
        assert!(joint.state.pending().is_some());
        assert!(joint.state.first_phase("overlap", &learner(5)).is_err());
        let done = joint
            .advanced(
                21,
                3,
                &change("replace-1", operation, ConfigurationPhase::Final),
            )
            .unwrap();
        assert!(done.state.retired.contains(&1));
        assert!(!done.state.participants().contains(&1));
        assert_eq!(done.state.records["replace-1"].first_index, 20);
        assert_eq!(done.state.records["replace-1"].final_index, Some(21));
        assert!(done.state.first_phase("reuse-1", &learner(1)).is_err());
        one.validate().unwrap();
        joint.validate().unwrap();
        done.validate().unwrap();
        assert!(done.is_consistent_extension_of(&one));
        assert!(done.is_consistent_extension_of(&joint));
        assert!(!joint.is_consistent_extension_of(&done));
    }

    #[test]
    fn learner_removal_is_single_phase_and_cannot_resurrect_from_new_admin_id() {
        let state = initial()
            .advanced(1, 1, &change("add", learner(4), ConfigurationPhase::Apply))
            .unwrap();
        let state = state
            .advanced(
                2,
                1,
                &change(
                    "remove",
                    AdminOperation::Remove { id: 4 },
                    ConfigurationPhase::Apply,
                ),
            )
            .unwrap();
        assert!(!state.state.learners.contains(&4));
        assert!(state.state.retired.contains(&4));
        state.validate().unwrap();
        assert_eq!(
            state.state.first_phase("again", &learner(4)).unwrap_err(),
            "member_id_retired"
        );
    }

    #[test]
    fn changed_payload_unadmitted_promotion_wrong_phase_and_legacy_group_refuse() {
        let state = initial()
            .advanced(1, 1, &change("add", learner(4), ConfigurationPhase::Apply))
            .unwrap();
        assert_eq!(
            state.state.first_phase("add", &learner(5)).unwrap_err(),
            "request_payload_changed"
        );
        assert_eq!(
            state.state.first_phase("add", &learner(4)).unwrap_err(),
            "request_already_recorded"
        );
        assert!(state
            .state
            .first_phase(
                "promote",
                &AdminOperation::SetVoters {
                    voters: vec![1, 2, 5]
                }
            )
            .is_err());
        assert!(state
            .advanced(
                2,
                1,
                &change("bad-phase", learner(5), ConfigurationPhase::Joint)
            )
            .is_err());
        assert!(state
            .advanced(
                2,
                1,
                &change("bad-final", learner(4), ConfigurationPhase::Final)
            )
            .is_err());
        let legacy = CommittedMembership::bootstrap(vec![1, 2, 3]).unwrap();
        legacy.validate().unwrap();
        assert_eq!(
            legacy.state.first_phase("add", &learner(4)).unwrap_err(),
            "explicit_group_identity_required"
        );
    }

    #[test]
    fn advertisement_history_cannot_change_group_genesis_or_known_prefix() {
        let state = initial()
            .advanced(
                5,
                2,
                &change("admitted", learner(4), ConfigurationPhase::Apply),
            )
            .unwrap();
        let mut wrong = state.clone();
        wrong.state.group_id = "group-b".into();
        wrong.validate().unwrap();
        assert!(!wrong.is_consistent_extension_of(&state));
        let mut forged = state.clone();
        forged.state.learners.insert(5);
        assert!(forged.validate().is_err());
        let mut rewritten = state.clone();
        rewritten
            .state
            .records
            .get_mut("admitted")
            .unwrap()
            .first_index = 4;
        rewritten
            .state
            .records
            .get_mut("admitted")
            .unwrap()
            .final_index = Some(4);
        rewritten.index = 4;
        rewritten.validate().unwrap();
        assert!(!rewritten.is_consistent_extension_of(&state));
        let other = initial()
            .advanced(
                6,
                2,
                &change("contradictory", learner(5), ConfigurationPhase::Apply),
            )
            .unwrap();
        assert!(!other.is_consistent_extension_of(&state));
    }

    #[test]
    fn legacy_hard_state_and_fixed_snapshot_metadata_decode_without_membership_field() {
        let hard: crate::HardState =
            serde_json::from_str(r#"{"current_term":4,"voted_for":2}"#).unwrap();
        assert!(hard.membership.is_none());
        let meta: crate::SnapshotMetadata = serde_json::from_str(
            r#"{"last_included_index":3,"last_included_term":2,"members":[1,2,3]}"#,
        )
        .unwrap();
        assert!(meta.membership.is_none());
        meta.validate().unwrap();
        assert!(!serde_json::to_string(&hard).unwrap().contains("membership"));
    }

    #[test]
    fn configuration_history_round_trips_and_cannot_escape_snapshot_boundary() {
        let state = initial()
            .advanced(3, 2, &change("add", learner(4), ConfigurationPhase::Apply))
            .unwrap();
        let encoded = serde_json::to_vec(&state).unwrap();
        let reopened: CommittedMembership = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(reopened, state);
        reopened.validate().unwrap();
        let mut metadata = crate::SnapshotMetadata {
            last_included_index: 3,
            last_included_term: 2,
            members: vec![1, 2, 3, 4],
            membership: Some(reopened),
        };
        metadata.validate().unwrap();
        metadata.last_included_index = 2;
        assert!(metadata.validate().is_err());
        metadata.last_included_index = 3;
        metadata.members.pop();
        assert!(metadata.validate().is_err());
    }

    #[test]
    fn ordinary_client_cannot_inject_configuration_entry_even_inside_batch() {
        let mut node = crate::RaftNode::new(1, vec![], 7);
        node.hard.current_term = 1;
        node.role = crate::Role::Leader {
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
        };
        let effects = node.step(crate::Input::ClientProposeBatch {
            commands: vec![
                crate::Command::NoOp,
                crate::Command::Configuration(change(
                    "forged",
                    learner(4),
                    ConfigurationPhase::Apply,
                )),
            ],
        });
        assert!(node.log.is_empty());
        assert_eq!(effects.len(), 2);
        assert!(effects
            .iter()
            .all(|effect| matches!(effect, crate::Effect::ProposeRejected { .. })));
    }

    fn ids(mask: u32) -> Vec<NodeId> {
        (0..5)
            .filter(|bit| mask & (1 << bit) != 0)
            .map(|bit| bit as NodeId)
            .collect()
    }

    #[test]
    fn every_five_node_old_new_vote_subset_matches_independent_bitmask_oracle() {
        for old in 1u32..32 {
            for new in 1u32..32 {
                let config = VoterConfig::Joint {
                    old: ids(old),
                    new: ids(new),
                };
                for votes in 0u32..32 {
                    // Independent representation: bitsets, not helper membership iteration.
                    let expected = (old & votes).count_ones() * 2 > old.count_ones()
                        && (new & votes).count_ones() * 2 > new.count_ones();
                    assert_eq!(
                        config.has_quorum(&ids(votes).into_iter().collect()),
                        expected,
                        "old={old:05b} new={new:05b} votes={votes:05b}"
                    );
                }
            }
        }
    }

    #[test]
    fn union_majority_old_only_new_only_learners_and_duplicates_cannot_replace_both_quorums() {
        let joint = VoterConfig::Joint {
            old: vec![1, 2, 3],
            new: vec![3, 4, 5],
        };
        assert!(!joint.has_quorum(&BTreeSet::from([1, 2, 3])));
        assert!(!joint.has_quorum(&BTreeSet::from([3, 4, 5])));
        assert!(!joint.has_quorum(&[1, 1, 3, 7, 8, 9].into_iter().collect()));
        assert!(joint.has_quorum(&BTreeSet::from([1, 3, 4])));
        assert_eq!(joint.voters(), BTreeSet::from([1, 2, 3, 4, 5]));
        assert!(!joint.contains(7));
    }

    #[test]
    fn joint_replication_watermark_matches_exhaustive_threshold_oracle() {
        for new_mask in 1u32..32 {
            let config = VoterConfig::Joint {
                old: vec![0, 1, 2],
                new: ids(new_mask),
            };
            for encoded in 0u32..1024 {
                let matched: BTreeMap<_, _> = (0..5)
                    .map(|id| (id, ((encoded >> (id * 2)) & 3) as u64))
                    .collect();
                let expected = (1..=3)
                    .rev()
                    .find(|index| {
                        let old_count = [0, 1, 2].iter().filter(|id| matched[id] >= *index).count();
                        let new = ids(new_mask);
                        let new_count = new.iter().filter(|id| matched[id] >= *index).count();
                        old_count >= 2 && new_count * 2 > new.len()
                    })
                    .unwrap_or(0);
                assert_eq!(config.replicated_index(&matched), expected);
            }
        }
    }

    #[test]
    fn malformed_sets_and_missing_peer_progress_fail_closed() {
        for voters in [vec![], vec![1, 1], vec![2, 1]] {
            let config = VoterConfig::Stable { voters };
            assert!(config.validate().is_err());
            assert!(!config.has_quorum(&BTreeSet::from([1, 2, 3])));
            assert_eq!(
                config.replicated_index(&BTreeMap::from([(1, 9), (2, 9)])),
                0
            );
        }
        let single = VoterConfig::Stable { voters: vec![0] };
        assert!(single.has_quorum(&BTreeSet::from([0])));
        assert_eq!(single.replicated_index(&BTreeMap::new()), 0);
        let joint = VoterConfig::Joint {
            old: vec![0],
            new: vec![1],
        };
        assert!(!joint.has_quorum(&BTreeSet::from([0])));
        assert_eq!(joint.replicated_index(&BTreeMap::from([(0, 9)])), 0);
        assert_eq!(joint.replicated_index(&BTreeMap::from([(0, 9), (1, 7)])), 7);
    }
}
