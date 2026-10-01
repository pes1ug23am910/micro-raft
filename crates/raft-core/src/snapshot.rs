//! Snapshot boundaries and bounded disk-backed transfer orchestration.
//!
//! Image decoding, hashing, fsync, and atomic publication belong to the shell.
//! A publication completion is part of its serialized effect batch: no unrelated
//! input may interleave between PublishSnapshot and SnapshotPublished.

use crate::{
    Effect, Entry, LogIndex, NodeId, RaftMessage, RaftNode, Role, SnapshotDescriptor,
    SnapshotStageResult, SnapshotTransferId, Term, MAX_SNAPSHOT_CHUNK_BYTES,
};

#[derive(Debug)]
pub(crate) struct OutgoingSnapshot {
    transfer: SnapshotTransferId,
    descriptor: SnapshotDescriptor,
    offset: u64,
    reading: bool,
    sent_end: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct IncomingSnapshot {
    transfer: SnapshotTransferId,
    descriptor: SnapshotDescriptor,
    next_offset: u64,
    pending: Option<(u64, u64, u64)>, // offset, end, contact round
}

#[derive(Debug)]
pub(crate) struct PublishingSnapshot {
    transfer: Option<SnapshotTransferId>,
    descriptor: SnapshotDescriptor,
    retained: Vec<Entry>,
    contact_round: u64,
}

pub fn valid_chunk(descriptor: &SnapshotDescriptor, offset: u64, len: usize, done: bool) -> bool {
    descriptor.validate().is_ok()
        && offset < descriptor.total_len
        && offset.is_multiple_of(MAX_SNAPSHOT_CHUNK_BYTES as u64)
        && len as u64 == (descriptor.total_len - offset).min(MAX_SNAPSHOT_CHUNK_BYTES as u64)
        && done == (offset + len as u64 == descriptor.total_len)
}

impl RaftNode {
    pub(crate) fn cancel_incoming_snapshot(&mut self, effects: &mut Vec<Effect>) {
        if let Some(active) = self.incoming_snapshot.take() {
            effects.push(Effect::CancelSnapshot {
                transfer: active.transfer,
            });
        }
    }

    fn snapshot_reject(&self, effects: &mut Vec<Effect>, reason: &'static str) {
        effects.push(Effect::SnapshotRejected { reason });
    }

    fn retained_suffix(&self, descriptor: &SnapshotDescriptor) -> Vec<Entry> {
        let boundary = descriptor.metadata.last_included_index;
        if self.log_term(boundary) == Some(descriptor.metadata.last_included_term) {
            self.log
                .iter()
                .filter(|entry| entry.index > boundary)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    pub(crate) fn on_compact(&mut self, descriptor: SnapshotDescriptor, effects: &mut Vec<Effect>) {
        let boundary = descriptor.metadata.last_included_index;
        if self.publishing_snapshot.is_some() || self.incoming_snapshot.is_some() {
            self.snapshot_reject(effects, "snapshot_busy");
            return;
        }
        if descriptor.validate().is_err()
            || !self.compaction_membership_matches(&descriptor)
            || boundary <= self.snapshot_index()
            || boundary > self.last_applied
            || boundary > self.commit_index
            || self.log_term(boundary) != Some(descriptor.metadata.last_included_term)
        {
            self.snapshot_reject(effects, "invalid_compaction_boundary");
            return;
        }
        let retained = self.retained_suffix(&descriptor);
        self.publishing_snapshot = Some(PublishingSnapshot {
            transfer: None,
            descriptor: descriptor.clone(),
            retained: retained.clone(),
            contact_round: 0,
        });
        effects.push(Effect::PublishSnapshot {
            transfer: None,
            descriptor,
            retained_entries: retained,
        });
    }

    pub(crate) fn on_snapshot_published(
        &mut self,
        transfer: Option<SnapshotTransferId>,
        descriptor: SnapshotDescriptor,
        effects: &mut Vec<Effect>,
    ) {
        let Some(pending) = &self.publishing_snapshot else {
            return;
        };
        if pending.transfer != transfer || pending.descriptor != descriptor {
            return;
        }
        let pending = self
            .publishing_snapshot
            .take()
            .expect("matched publication");
        self.log = pending.retained;
        self.snapshot = Some(descriptor.clone());
        self.outgoing_snapshots.clear();
        if let Some(transfer) = transfer {
            if let Some(membership) = &descriptor.metadata.membership {
                if self.hard.membership.as_ref() != Some(membership) {
                    self.hard.membership = Some(membership.clone());
                    effects.push(Effect::PersistHardState(self.hard.clone()));
                }
            }
            self.commit_index = descriptor.metadata.last_included_index;
            self.refresh_effective_membership(effects);
            self.last_applied = self.commit_index;
            self.incoming_snapshot = None;
            effects.push(Effect::ApplySnapshot {
                descriptor: descriptor.clone(),
            });
            self.snapshot_reply(
                transfer,
                descriptor.total_len,
                true,
                true,
                descriptor.metadata.last_included_index,
                pending.contact_round,
                effects,
            );
        }
    }

    pub(crate) fn send_snapshot_chunk(&mut self, to: NodeId, effects: &mut Vec<Effect>) {
        if !self.is_leader() {
            return;
        }
        let Some(descriptor) = self.snapshot.clone() else {
            return;
        };
        if !self.outgoing_snapshots.contains_key(&to) {
            let Some(sequence) = self.snapshot_sequence.checked_add(1) else {
                self.snapshot_reject(effects, "snapshot_transfer_exhausted");
                return;
            };
            self.snapshot_sequence = sequence;
            self.outgoing_snapshots.insert(
                to,
                OutgoingSnapshot {
                    transfer: SnapshotTransferId {
                        leader_id: self.id,
                        term: self.hard.current_term,
                        incarnation: self.campaign_incarnation,
                        sequence,
                    },
                    descriptor,
                    offset: 0,
                    reading: false,
                    sent_end: None,
                },
            );
        }
        let active = self
            .outgoing_snapshots
            .get_mut(&to)
            .expect("created transfer");
        if active.reading {
            return;
        }
        active.reading = true;
        effects.push(Effect::ReadSnapshotChunk {
            to,
            transfer: active.transfer,
            descriptor: active.descriptor.clone(),
            offset: active.offset,
            max_len: MAX_SNAPSHOT_CHUNK_BYTES,
        });
    }

    pub(crate) fn on_snapshot_chunk_read(
        &mut self,
        to: NodeId,
        transfer: SnapshotTransferId,
        descriptor: SnapshotDescriptor,
        offset: u64,
        data: Vec<u8>,
        effects: &mut Vec<Effect>,
    ) {
        if !self.is_leader() || transfer.term != self.hard.current_term {
            return;
        }
        let Some(active) = self.outgoing_snapshots.get_mut(&to) else {
            return;
        };
        if transfer != active.transfer
            || descriptor != active.descriptor
            || offset != active.offset
            || !active.reading
        {
            return;
        }
        active.reading = false;
        let done = offset.checked_add(data.len() as u64) == Some(descriptor.total_len);
        if !valid_chunk(&descriptor, offset, data.len(), done) {
            self.snapshot_reject(effects, "invalid_snapshot_read");
            return;
        }
        active.sent_end = Some(offset + data.len() as u64);
        effects.push(Effect::Send {
            to,
            msg: RaftMessage::InstallSnapshot {
                transfer,
                descriptor,
                offset,
                data,
                done,
                contact_round: self.contact_round,
            },
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn snapshot_reply(
        &self,
        transfer: SnapshotTransferId,
        next_offset: u64,
        accepted: bool,
        installed: bool,
        match_index: LogIndex,
        contact_round: u64,
        effects: &mut Vec<Effect>,
    ) {
        effects.push(Effect::Send {
            to: transfer.leader_id,
            msg: RaftMessage::InstallSnapshotReply {
                term: self.hard.current_term,
                transfer,
                next_offset,
                match_index,
                accepted,
                installed,
                contact_round,
            },
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_install_snapshot(
        &mut self,
        transfer: SnapshotTransferId,
        descriptor: SnapshotDescriptor,
        offset: u64,
        data: Vec<u8>,
        done: bool,
        contact_round: u64,
        effects: &mut Vec<Effect>,
    ) {
        if transfer.term < self.hard.current_term {
            self.snapshot_reply(transfer, 0, false, false, self.last_log_index(), 0, effects);
            return;
        }
        let same_image = self.snapshot.as_ref() == Some(&descriptor);
        let boundary = descriptor.metadata.last_included_index;
        if self.publishing_snapshot.is_some()
            || (!same_image && (boundary <= self.last_applied || boundary < self.commit_index))
        {
            self.snapshot_reply(transfer, 0, false, false, self.last_log_index(), 0, effects);
            return;
        }
        if let Some((last, last_descriptor)) = &self.last_snapshot_transfer {
            let older = transfer.term < last.term
                || (transfer.term == last.term
                    && (transfer.leader_id != last.leader_id
                        || transfer.incarnation != last.incarnation
                        || transfer.sequence < last.sequence
                        || descriptor.metadata.last_included_index
                            < last_descriptor.metadata.last_included_index));
            if older || (transfer == *last && descriptor != *last_descriptor) {
                self.snapshot_reply(transfer, 0, false, false, self.last_log_index(), 0, effects);
                return;
            }
        }
        let same_transfer = self
            .incoming_snapshot
            .as_ref()
            .is_some_and(|active| active.transfer == transfer && active.descriptor == descriptor);
        if !same_transfer && offset != 0 && !same_image {
            self.snapshot_reply(transfer, 0, false, false, self.last_log_index(), 0, effects);
            return;
        }
        if !same_transfer {
            self.cancel_incoming_snapshot(effects);
            self.last_snapshot_transfer = Some((transfer, descriptor.clone()));
            self.incoming_snapshot = Some(IncomingSnapshot {
                transfer,
                descriptor: descriptor.clone(),
                next_offset: 0,
                pending: None,
            });
        }
        let active = self.incoming_snapshot.as_mut().expect("active transfer");
        if active.pending.is_some() || (offset > active.next_offset && !same_image) {
            self.snapshot_reply(transfer, 0, false, false, self.last_log_index(), 0, effects);
            return;
        }
        active.pending = Some((offset, offset + data.len() as u64, contact_round));
        self.become_follower(effects);
        self.reset_election_deadline();
        self.leader_hint = Some(transfer.leader_id);
        self.last_leader_contact_ms = Some(self.now_ms);
        effects.push(Effect::StageSnapshotChunk {
            transfer,
            descriptor,
            offset,
            data,
            done,
        });
    }

    pub(crate) fn on_snapshot_chunk_staged(
        &mut self,
        transfer: SnapshotTransferId,
        descriptor: SnapshotDescriptor,
        offset: u64,
        result: SnapshotStageResult,
        effects: &mut Vec<Effect>,
    ) {
        let Some(active) = &self.incoming_snapshot else {
            return;
        };
        if active.transfer != transfer
            || active.descriptor != descriptor
            || transfer.term != self.hard.current_term
        {
            return;
        }
        let Some((expected_offset, expected_end, contact_round)) = active.pending else {
            return;
        };
        if expected_offset != offset {
            return;
        }
        let previous_next = active.next_offset;
        self.incoming_snapshot.as_mut().expect("active").pending = None;
        let SnapshotStageResult::Accepted {
            next_offset,
            complete,
        } = result
        else {
            self.snapshot_reply(
                transfer,
                previous_next,
                false,
                false,
                self.last_log_index(),
                0,
                effects,
            );
            return;
        };
        // A duplicate may report the already staged prefix; an exact installed
        // image may finish immediately. No other forward callback jump is valid.
        let expected_next = if self.snapshot.as_ref() == Some(&descriptor) {
            descriptor.total_len
        } else {
            previous_next.max(expected_end)
        };
        if next_offset != expected_next
            || next_offset > descriptor.total_len
            || complete != (next_offset == descriptor.total_len)
            || (!complete && !next_offset.is_multiple_of(MAX_SNAPSHOT_CHUNK_BYTES as u64))
        {
            self.cancel_incoming_snapshot(effects);
            self.snapshot_reply(transfer, 0, false, false, self.last_log_index(), 0, effects);
            return;
        }
        self.incoming_snapshot.as_mut().expect("active").next_offset = next_offset;
        if !complete {
            self.snapshot_reply(
                transfer,
                next_offset,
                true,
                false,
                self.last_log_index(),
                contact_round,
                effects,
            );
            return;
        }
        // Exact bytes were checked by the shell against the installed image.
        // Replaying its final ACK must not reset application state or republish.
        if self.snapshot.as_ref() == Some(&descriptor) {
            self.incoming_snapshot = None;
            self.snapshot_reply(
                transfer,
                next_offset,
                true,
                true,
                descriptor.metadata.last_included_index,
                contact_round,
                effects,
            );
            return;
        }
        let boundary = descriptor.metadata.last_included_index;
        if boundary <= self.last_applied || boundary < self.commit_index {
            self.cancel_incoming_snapshot(effects);
            self.snapshot_reply(
                transfer,
                next_offset,
                false,
                false,
                self.last_log_index(),
                0,
                effects,
            );
            return;
        }
        let retained = self.retained_suffix(&descriptor);
        self.publishing_snapshot = Some(PublishingSnapshot {
            transfer: Some(transfer),
            descriptor: descriptor.clone(),
            retained: retained.clone(),
            contact_round,
        });
        effects.push(Effect::PublishSnapshot {
            transfer: Some(transfer),
            descriptor,
            retained_entries: retained,
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_install_snapshot_reply(
        &mut self,
        from: NodeId,
        term: Term,
        transfer: SnapshotTransferId,
        next_offset: u64,
        match_index: LogIndex,
        accepted: bool,
        installed: bool,
        contact_round: u64,
        effects: &mut Vec<Effect>,
    ) {
        if term != self.hard.current_term || transfer.term != term || !self.is_leader() {
            return;
        }
        let Some(active) = self.outgoing_snapshots.get(&from) else {
            return;
        };
        if active.transfer != transfer || active.sent_end.is_none() {
            return;
        }
        if !accepted {
            if match_index >= active.descriptor.metadata.last_included_index
                && match_index <= self.last_log_index()
            {
                if let Role::Leader { next_index, .. } = &mut self.role {
                    next_index.insert(from, match_index.saturating_add(1));
                }
            }
            // A reboot can erase an unselected partial stage. Never keep
            // retrying its nonzero offset forever: a fresh transfer restarts
            // from zero and also supersedes a receiver's rejected stage.
            self.outgoing_snapshots.remove(&from);
            return;
        }
        let valid = if installed {
            next_offset == active.descriptor.total_len
                && match_index == active.descriptor.metadata.last_included_index
        } else {
            Some(next_offset) == active.sent_end
                && next_offset < active.descriptor.total_len
                && next_offset > active.offset
        };
        if !valid {
            return;
        }
        if contact_round != 0 && contact_round == self.contact_round {
            self.quorum_contacts.insert(from);
        }
        if installed {
            self.outgoing_snapshots.remove(&from);
            self.on_append_entries_reply(from, term, true, match_index, 0, None, effects);
            self.send_append_entries(effects);
        } else {
            let active = self
                .outgoing_snapshots
                .get_mut(&from)
                .expect("matched transfer");
            active.offset = next_offset;
            active.sent_end = None;
            active.reading = false;
            self.send_snapshot_chunk(from, effects);
        }
    }
}
