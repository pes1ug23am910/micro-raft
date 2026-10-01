//! Virtual snapshot shell with an independent full-history witness.
//!
//! This is deliberately not the production image parser or filesystem model.
//! Its immutable catalogue represents decodable images; an incoming image is
//! recognized only after every byte and the complete SHA256 match. Historical
//! entries remain in the witness after production core/log compaction.

use crate::*;
use raft_core::{SnapshotMetadata, SnapshotStageResult, MAX_SNAPSHOT_BYTES};
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub(crate) struct VirtualImage {
    pub descriptor: SnapshotDescriptor,
    pub prefix: Vec<Entry>,
    bytes: Vec<u8>,
}

pub(crate) struct VirtualStage {
    transfer: SnapshotTransferId,
    descriptor: SnapshotDescriptor,
    bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotFault {
    CrashAfterStage,
    CrashBeforePublication,
    CrashAfterPublication,
    /// Executed defective publication path; the original durability audit must fail.
    SkipDurablePublication,
    /// Executed defective image restoration; full historical safety must fail.
    CorruptPublishedHistory,
}

impl Sim {
    pub fn set_snapshot_fault(&mut self, id: NodeId, fault: SnapshotFault) {
        assert!(self.nodes.contains_key(&id));
        self.snapshot_faults.insert(id, fault);
    }

    pub fn snapshot_fault_hits(&self) -> u64 {
        self.snapshot_fault_hits
    }
    pub fn snapshots_installed(&self) -> u64 {
        self.snapshots_installed
    }
    pub fn staged_snapshot_bytes(&self, id: NodeId) -> usize {
        self.nodes[&id]
            .snapshot_stage
            .as_ref()
            .map_or(0, |stage| stage.bytes.len())
    }

    /// Prepare an image from this node's independently audited applied prefix.
    /// Padding forces multi-chunk transfers without inventing extra log entries.
    pub fn compact(&mut self, id: NodeId, minimum_image_bytes: usize) -> Vec<Effect> {
        let node = &self.nodes[&id];
        assert!(node.alive && node.core.last_applied > node.core.snapshot_index());
        let boundary = node.core.last_applied;
        let mut prefix = node.core_prefix.clone();
        prefix.extend(
            node.core
                .log
                .iter()
                .filter(|entry| entry.index <= boundary)
                .cloned(),
        );
        for entry in &prefix {
            self.committed
                .assert_applied(self.seed, id, entry.index, &entry.command);
        }
        let metadata = SnapshotMetadata {
            membership: node.core.hard.membership.clone(),
            last_included_index: boundary,
            last_included_term: node.core.log_term(boundary).expect("applied boundary term"),
            members: node
                .core
                .committed_membership()
                .state
                .participants()
                .into_iter()
                .collect(),
        };
        let mut bytes = format!("SIMIMAGE1:{metadata:?}:{prefix:?}").into_bytes();
        bytes.resize(bytes.len().max(minimum_image_bytes).max(48), 0);
        assert!(bytes.len() <= MAX_SNAPSHOT_BYTES);
        let descriptor = SnapshotDescriptor {
            metadata,
            total_len: bytes.len() as u64,
            sha256: Sha256::digest(&bytes).into(),
        };
        let image = VirtualImage {
            descriptor: descriptor.clone(),
            prefix,
            bytes,
        };
        self.snapshot_images.insert(descriptor.sha256, image);
        self.read_input(id, Input::Compact { descriptor })
    }

    fn callback(&mut self, id: NodeId, input: Input) {
        let effects = self
            .nodes
            .get_mut(&id)
            .expect("known node")
            .core
            .step(input);
        self.execute_effects(id, effects);
    }

    pub(crate) fn execute_snapshot_effect(&mut self, id: NodeId, effect: Effect) {
        match effect {
            Effect::ReadSnapshotChunk {
                to,
                transfer,
                descriptor,
                offset,
                max_len,
            } => {
                let image = self.nodes[&id]
                    .disk_snapshot
                    .as_ref()
                    .expect("selected durable image");
                assert_eq!(image.descriptor, descriptor);
                let end = (offset as usize + max_len).min(image.bytes.len());
                let data = image.bytes[offset as usize..end].to_vec();
                self.callback(
                    id,
                    Input::SnapshotChunkRead {
                        to,
                        transfer,
                        descriptor,
                        offset,
                        data,
                    },
                );
            }
            Effect::StageSnapshotChunk {
                transfer,
                descriptor,
                offset,
                data,
                done,
            } => {
                let valid_shape =
                    raft_core::snapshot::valid_chunk(&descriptor, offset, data.len(), done);
                let image = self.snapshot_images.get(&descriptor.sha256);
                let recognized = image.is_some_and(|image| image.descriptor == descriptor);
                let node = self.nodes.get_mut(&id).expect("known node");
                let same_installed = node.disk_snapshot.as_ref().is_some_and(|image| {
                    image.descriptor == descriptor
                        && image
                            .bytes
                            .get(offset as usize..offset as usize + data.len())
                            == Some(data.as_slice())
                });
                let result = if valid_shape && same_installed {
                    SnapshotStageResult::Accepted {
                        next_offset: descriptor.total_len,
                        complete: true,
                    }
                } else if valid_shape && recognized {
                    if node.snapshot_stage.as_ref().is_none_or(|stage| {
                        stage.transfer != transfer || stage.descriptor != descriptor
                    }) {
                        assert_eq!(offset, 0, "new virtual stage starts at zero");
                        node.snapshot_stage = Some(VirtualStage {
                            transfer,
                            descriptor: descriptor.clone(),
                            bytes: Vec::new(),
                        });
                    }
                    let stage = node.snapshot_stage.as_mut().expect("created stage");
                    let accepted = if offset as usize == stage.bytes.len() {
                        stage.bytes.extend_from_slice(&data);
                        true
                    } else {
                        stage
                            .bytes
                            .get(offset as usize..offset as usize + data.len())
                            == Some(data.as_slice())
                    };
                    let complete = stage.bytes.len() as u64 == descriptor.total_len;
                    if accepted
                        && (!complete
                            || (stage.bytes == image.expect("recognized").bytes
                                && <[u8; 32]>::from(Sha256::digest(&stage.bytes))
                                    == descriptor.sha256))
                    {
                        SnapshotStageResult::Accepted {
                            next_offset: stage.bytes.len() as u64,
                            complete,
                        }
                    } else {
                        SnapshotStageResult::Rejected
                    }
                } else {
                    SnapshotStageResult::Rejected
                };
                if self.snapshot_faults.get(&id) == Some(&SnapshotFault::CrashAfterStage) {
                    self.snapshot_faults.remove(&id);
                    self.snapshot_fault_hits += 1;
                    self.crash(id);
                    return;
                }
                self.callback(
                    id,
                    Input::SnapshotChunkStaged {
                        transfer,
                        descriptor,
                        offset,
                        result,
                    },
                );
            }
            Effect::PublishSnapshot {
                transfer,
                descriptor,
                retained_entries,
            } => {
                let fault = self.snapshot_faults.remove(&id);
                if fault.is_some() {
                    self.snapshot_fault_hits += 1;
                }
                if fault == Some(SnapshotFault::CrashBeforePublication) {
                    self.crash(id);
                    return;
                }
                let mut image = self
                    .snapshot_images
                    .get(&descriptor.sha256)
                    .expect("prepared complete image")
                    .clone();
                assert_eq!(image.descriptor, descriptor);
                let node = self.nodes.get_mut(&id).expect("known node");
                if transfer.is_some() {
                    let stage = node
                        .snapshot_stage
                        .as_ref()
                        .expect("durable complete stage");
                    assert_eq!(Some(stage.transfer), transfer);
                    assert_eq!(stage.bytes, image.bytes);
                }
                // Independently derive suffix from virtual disk, not core's effect.
                let boundary = descriptor.metadata.last_included_index;
                let term = node
                    .disk_log
                    .iter()
                    .find(|entry| entry.index == boundary)
                    .map(|entry| entry.term)
                    .or_else(|| {
                        node.disk_snapshot
                            .as_ref()
                            .filter(|old| old.descriptor.metadata.last_included_index == boundary)
                            .map(|old| old.descriptor.metadata.last_included_term)
                    });
                let expected: Vec<_> = if term == Some(descriptor.metadata.last_included_term) {
                    node.disk_log
                        .iter()
                        .filter(|entry| entry.index > boundary)
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                };
                assert_eq!(
                    expected, retained_entries,
                    "core and virtual disk disagree on retained suffix"
                );
                if fault == Some(SnapshotFault::CorruptPublishedHistory) {
                    image.prefix[0].command = Command::Delete {
                        key: "injected-wrong-history".into(),
                    };
                }
                if fault != Some(SnapshotFault::SkipDurablePublication) {
                    node.disk_log = retained_entries;
                    node.disk_snapshot = Some(image.clone());
                }
                if fault == Some(SnapshotFault::CrashAfterPublication) {
                    self.crash(id);
                    return;
                }
                node.core_prefix = image.prefix.clone();
                self.callback(
                    id,
                    Input::SnapshotPublished {
                        transfer,
                        descriptor,
                    },
                );
            }
            Effect::ApplySnapshot { descriptor } => {
                self.assert_fully_durable(id, "ApplySnapshot");
                let node = self.nodes.get_mut(&id).expect("known node");
                let image = node.disk_snapshot.as_ref().expect("published snapshot");
                assert_eq!(image.descriptor, descriptor);
                node.applied = image
                    .prefix
                    .iter()
                    .map(|entry| (entry.index, entry.command.clone()))
                    .collect();
                node.snapshot_stage = None;
                self.snapshots_installed += 1;
            }
            Effect::CancelSnapshot { transfer } => {
                let node = self.nodes.get_mut(&id).expect("known node");
                if node
                    .snapshot_stage
                    .as_ref()
                    .is_some_and(|stage| stage.transfer == transfer)
                {
                    node.snapshot_stage = None;
                }
            }
            Effect::SnapshotRejected { .. } => {}
            _ => unreachable!("snapshot effect dispatcher"),
        }
    }
}
