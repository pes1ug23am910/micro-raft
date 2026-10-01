use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cached {
    pub sequence: u64,
    pub digest: [u8; 32],
    #[serde(with = "crate::compact_bytes")]
    pub payload: Vec<u8>,
    pub receipt: Receipt,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    pub id: SessionId,
    pub nonce: String,
    pub registered: Receipt,
    pub closed: Option<Receipt>,
    pub keys: BTreeMap<String, Cached>,
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardData {
    /// Explicit None cells preserve tombstones across transfer and cleanup.
    pub cells: BTreeMap<String, Option<String>>,
    pub sessions: BTreeMap<String, Session>,
    pub nonces: BTreeMap<String, SessionId>,
}
impl ShardData {
    pub fn retention(&self) -> Retention {
        Retention {
            sessions: self.sessions.len(),
            retry_keys: self.sessions.values().map(|s| s.keys.len()).sum(),
            payload_bytes: self
                .sessions
                .values()
                .flat_map(|s| s.keys.values())
                .map(|c| c.payload.len())
                .sum(),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardImage {
    pub version: u32,
    pub proof_identity: Move,
    pub fence: Origin,
    pub shard_count: u16,
    pub data: ShardData,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredState {
    pub activation: ActivationProof,
    pub cleaned: Origin,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum Slot {
    Active {
        epoch: u64,
        activation: Option<ActivationProof>,
    },
    Fenced {
        image: ImageProof,
        previous_activation: Option<ActivationProof>,
    },
    Installing {
        image: ImageProof,
        #[serde(with = "crate::compact_bytes")]
        bytes: Vec<u8>,
        previous_retired: Option<RetiredState>,
    },
    Installed {
        installation: InstalledProof,
        previous_retired: Option<RetiredState>,
    },
    Retired {
        activation: ActivationProof,
        cleaned: Origin,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shard {
    pub slot: Slot,
    pub data: Option<ShardData>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum DataCommand {
    Initialize {
        shard_count: u16,
        groups: Vec<GroupId>,
        owned: Vec<ShardId>,
    },
    Register {
        shard: ShardId,
        epoch: u64,
        nonce: String,
    },
    Close {
        shard: ShardId,
        epoch: u64,
        session: SessionId,
    },
    Mutate {
        shard: ShardId,
        epoch: u64,
        session: SessionId,
        key: String,
        sequence: u64,
        value: Option<String>,
    },
    Fence {
        movement: Move,
    },
    BeginInstall {
        image: ImageProof,
    },
    ResetInstall {
        image: ImageProof,
    },
    Chunk {
        id: TransferId,
        offset: u64,
        bytes: Vec<u8>,
    },
    FinishInstall {
        id: TransferId,
    },
    Activate {
        ownership: OwnershipProof,
    },
    Cleanup {
        activation: ActivationProof,
    },
    AbortTransfer {
        abort: AbortProof,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataReply {
    Initialized { origin: Origin },
    Receipt(Receipt),
    Fenced(ImageProof),
    Progress { id: TransferId, next_offset: u64 },
    Installed(InstalledProof),
    Activated(ActivationProof),
    Cleaned { id: TransferId, origin: Origin },
    Aborted(AbortRecord),
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataMachine {
    pub group: GroupId,
    pub applied: u64,
    pub initialized_at: Option<Origin>,
    pub shard_count: u16,
    pub groups: Vec<GroupId>,
    pub shards: BTreeMap<ShardId, Shard>,
    pub aborted: BTreeMap<String, AbortRecord>,
}

impl DataMachine {
    pub fn new(group: GroupId) -> Result<Self, Error> {
        if group == CONTROLLER {
            return Err(Error::WrongGroup);
        }
        Ok(Self {
            group,
            applied: 0,
            initialized_at: None,
            shard_count: 0,
            groups: vec![],
            shards: BTreeMap::new(),
            aborted: BTreeMap::new(),
        })
    }
    pub fn advance(&mut self, index: u64) -> Result<(), String> {
        if self.applied.checked_add(1) != Some(index) {
            return Err("data application gap".into());
        }
        self.applied = index;
        Ok(())
    }
    pub fn apply(
        &mut self,
        index: u64,
        command: DataCommand,
    ) -> Result<Result<DataReply, Error>, String> {
        if self.applied.checked_add(1) != Some(index) {
            return Err("data application gap".into());
        }
        let mut next = self.clone();
        next.applied = index;
        let result = next.execute(index, command).and_then(|reply| {
            next.validate().map_err(|_| Error::Capacity)?;
            Ok(reply)
        });
        if result.is_ok() {
            *self = next;
        } else {
            self.applied = index;
        }
        Ok(result)
    }
    fn active(&self, shard: ShardId, epoch: u64) -> Result<&ShardData, Error> {
        let state = self.shards.get(&shard).ok_or(Error::StaleRoute)?;
        match state.slot {
            Slot::Active { epoch: current, .. } if current == epoch => {
                state.data.as_ref().ok_or(Error::Unavailable)
            }
            Slot::Active { .. } | Slot::Retired { .. } => Err(Error::StaleRoute),
            _ => Err(Error::Unavailable),
        }
    }
    fn active_mut(&mut self, shard: ShardId, epoch: u64) -> Result<&mut ShardData, Error> {
        self.active(shard, epoch)?;
        self.shards
            .get_mut(&shard)
            .and_then(|s| s.data.as_mut())
            .ok_or(Error::Unavailable)
    }
    /// Runtime may call only after a completed ReadIndex/application barrier.
    pub fn read(&self, shard: ShardId, epoch: u64, key: &str) -> Result<Option<String>, Error> {
        if key_shard(key, self.shard_count) != Some(shard) {
            return Err(Error::InvalidInput);
        }
        Ok(self.active(shard, epoch)?.cells.get(key).cloned().flatten())
    }
    /// Frozen source bytes are deterministic and unchanged until proven cleanup.
    pub fn export(&self, id: &TransferId, offset: u64) -> Result<(ImageProof, Vec<u8>), Error> {
        let shard = self.shards.get(&id.shard).ok_or(Error::WrongTransfer)?;
        let Slot::Fenced { image, .. } = &shard.slot else {
            return Err(Error::WrongPhase);
        };
        if image.movement.id != *id {
            return Err(Error::WrongTransfer);
        }
        let body = ShardImage {
            version: 1,
            proof_identity: image.movement.clone(),
            fence: image.fence,
            shard_count: self.shard_count,
            data: shard.data.clone().ok_or(Error::Unavailable)?,
        };
        let bytes = canonical_bytes(&body, MAX_TRANSFER_BYTES)?;
        if bytes.len() as u64 != image.bytes || sha256(&bytes) != image.sha256 {
            return Err(Error::PayloadMismatch);
        }
        let offset = usize::try_from(offset).map_err(|_| Error::ChunkOffset)?;
        if offset > bytes.len() {
            return Err(Error::ChunkOffset);
        }
        Ok((
            image.clone(),
            bytes[offset..bytes.len().min(offset + CHUNK_BYTES)].to_vec(),
        ))
    }
    fn execute(&mut self, index: u64, command: DataCommand) -> Result<DataReply, Error> {
        let origin = Origin {
            group: self.group,
            index,
        };
        if let DataCommand::Initialize {
            shard_count,
            groups,
            owned,
        } = command
        {
            if self.initialized_at.is_some() {
                return Err(Error::AlreadyInitialized);
            }
            if shard_count == 0
                || usize::from(shard_count) > MAX_SHARDS
                || groups.is_empty()
                || groups.len() > MAX_GROUPS
                || groups.contains(&CONTROLLER)
                || !groups.contains(&self.group)
                || groups.windows(2).any(|p| p[0] >= p[1])
                || owned.windows(2).any(|p| p[0] >= p[1])
                || owned.iter().any(|s| *s >= shard_count)
            {
                return Err(Error::InvalidInput);
            }
            self.shard_count = shard_count;
            self.groups = groups;
            self.initialized_at = Some(origin);
            for shard in owned {
                self.shards.insert(
                    shard,
                    Shard {
                        slot: Slot::Active {
                            epoch: 1,
                            activation: None,
                        },
                        data: Some(ShardData::default()),
                    },
                );
            }
            return Ok(DataReply::Initialized { origin });
        }
        if self.initialized_at.is_none() {
            return Err(Error::NotInitialized);
        }
        // An abort remains a permanent fence against delayed work, including
        // operations already queued by another coordinator before it observed
        // the controller's terminal decision.
        let transfer = match &command {
            DataCommand::Fence { movement } => Some(&movement.id),
            DataCommand::BeginInstall { image } | DataCommand::ResetInstall { image } => {
                Some(&image.movement.id)
            }
            DataCommand::Chunk { id, .. } | DataCommand::FinishInstall { id } => Some(id),
            DataCommand::Activate { ownership } => Some(&ownership.installation.image.movement.id),
            DataCommand::Cleanup { activation } => {
                Some(&activation.ownership.installation.image.movement.id)
            }
            _ => None,
        };
        if transfer.is_some_and(|id| self.aborted.contains_key(&id.key())) {
            return Err(Error::Aborted);
        }
        match command {
            DataCommand::AbortTransfer { abort } => self.abort_transfer(origin, abort),
            DataCommand::Register {
                shard,
                epoch,
                nonce,
            } => {
                if nonce.is_empty() || nonce.len() > 128 {
                    return Err(Error::InvalidInput);
                }
                let data = self.active_mut(shard, epoch)?;
                if let Some(session) = data.nonces.get(&nonce) {
                    return Ok(DataReply::Receipt(
                        data.sessions[&session.key()].registered.clone(),
                    ));
                }
                let receipt = Receipt {
                    origin,
                    shard,
                    epoch,
                    session: Some(origin),
                    sequence: None,
                };
                data.nonces.insert(nonce.clone(), origin);
                data.sessions.insert(
                    origin.key(),
                    Session {
                        id: origin,
                        nonce,
                        registered: receipt.clone(),
                        closed: None,
                        keys: BTreeMap::new(),
                    },
                );
                Ok(DataReply::Receipt(receipt))
            }
            DataCommand::Close {
                shard,
                epoch,
                session,
            } => {
                let data = self.active_mut(shard, epoch)?;
                let state = data
                    .sessions
                    .get_mut(&session.key())
                    .ok_or(Error::UnknownSession)?;
                if let Some(closed) = &state.closed {
                    return Ok(DataReply::Receipt(closed.clone()));
                }
                let receipt = Receipt {
                    origin,
                    shard,
                    epoch,
                    session: Some(session),
                    sequence: None,
                };
                state.closed = Some(receipt.clone());
                Ok(DataReply::Receipt(receipt))
            }
            DataCommand::Mutate {
                shard,
                epoch,
                session,
                key,
                sequence,
                value,
            } => {
                if key_shard(&key, self.shard_count) != Some(shard)
                    || sequence == 0
                    || value.as_ref().is_some_and(|v| v.len() > MAX_VALUE_BYTES)
                {
                    return Err(Error::InvalidInput);
                }
                let payload = canonical_bytes(&value, MAX_VALUE_BYTES * 6 + 32)?;
                let digest = sha256(&payload);
                let data = self.active_mut(shard, epoch)?;
                let state = data
                    .sessions
                    .get_mut(&session.key())
                    .ok_or(Error::UnknownSession)?;
                if state.closed.is_some() {
                    return Err(Error::SessionClosed);
                }
                if let Some(previous) = state.keys.get(&key) {
                    if sequence == previous.sequence {
                        if previous.digest == digest && previous.payload == payload {
                            return Ok(DataReply::Receipt(previous.receipt.clone()));
                        }
                        return Err(Error::PayloadMismatch);
                    }
                    if sequence < previous.sequence {
                        return Err(Error::StaleSequence);
                    }
                    if previous.sequence.checked_add(1) != Some(sequence) {
                        return Err(Error::SequenceGap);
                    }
                } else if sequence != 1 {
                    return Err(Error::SequenceGap);
                }
                let receipt = Receipt {
                    origin,
                    shard,
                    epoch,
                    session: Some(session),
                    sequence: Some(sequence),
                };
                state.keys.insert(
                    key.clone(),
                    Cached {
                        sequence,
                        digest,
                        payload,
                        receipt: receipt.clone(),
                    },
                );
                data.cells.insert(key, value);
                Ok(DataReply::Receipt(receipt))
            }
            DataCommand::Fence { movement } => {
                if !self.movement_valid(&movement) || movement.source != self.group {
                    return Err(Error::WrongTransfer);
                }
                let shard = self
                    .shards
                    .get(&movement.id.shard)
                    .ok_or(Error::StaleRoute)?;
                if let Slot::Fenced { image, .. } = &shard.slot {
                    return if image.movement == movement {
                        Ok(DataReply::Fenced(image.clone()))
                    } else {
                        Err(Error::WrongTransfer)
                    };
                }
                let previous_activation = match &shard.slot {
                    Slot::Active { activation, .. } => activation.clone(),
                    _ => None,
                };
                let data = self
                    .active(movement.id.shard, movement.previous_epoch)?
                    .clone();
                let body = ShardImage {
                    version: 1,
                    proof_identity: movement.clone(),
                    fence: origin,
                    shard_count: self.shard_count,
                    data,
                };
                let bytes = canonical_bytes(&body, MAX_TRANSFER_BYTES)?;
                let image = ImageProof {
                    movement: movement.clone(),
                    fence: origin,
                    bytes: bytes.len() as u64,
                    sha256: sha256(&bytes),
                    retention: body.data.retention(),
                };
                self.shards
                    .get_mut(&movement.id.shard)
                    .expect("checked shard")
                    .slot = Slot::Fenced {
                    image: image.clone(),
                    previous_activation,
                };
                Ok(DataReply::Fenced(image))
            }
            DataCommand::BeginInstall { image } => {
                if !image.valid()
                    || !self.movement_valid(&image.movement)
                    || image.movement.destination != self.group
                {
                    return Err(Error::WrongTransfer);
                }
                if let Some(shard) = self.shards.get(&image.movement.id.shard) {
                    match &shard.slot {
                        Slot::Installing {
                            image: old, bytes, ..
                        } if *old == image => {
                            return Ok(DataReply::Progress {
                                id: image.movement.id,
                                next_offset: bytes.len() as u64,
                            })
                        }
                        Slot::Installed { installation, .. } if installation.image == image => {
                            return Ok(DataReply::Installed(installation.clone()))
                        }
                        Slot::Retired { activation, .. }
                            if activation.ownership.installation.image.movement.id.epoch
                                < image.movement.id.epoch => {}
                        _ => return Err(Error::WrongPhase),
                    }
                }
                if self
                    .shards
                    .values()
                    .any(|s| matches!(s.slot, Slot::Installing { .. } | Slot::Installed { .. }))
                {
                    return Err(Error::Busy);
                }
                let previous_retired =
                    self.shards
                        .get(&image.movement.id.shard)
                        .and_then(|shard| match &shard.slot {
                            Slot::Retired {
                                activation,
                                cleaned,
                            } => Some(RetiredState {
                                activation: activation.clone(),
                                cleaned: *cleaned,
                            }),
                            _ => None,
                        });
                let id = image.movement.id.clone();
                self.shards.insert(
                    id.shard,
                    Shard {
                        slot: Slot::Installing {
                            image,
                            bytes: Vec::new(),
                            previous_retired,
                        },
                        data: None,
                    },
                );
                Ok(DataReply::Progress { id, next_offset: 0 })
            }
            DataCommand::Chunk { id, offset, bytes } => {
                if bytes.is_empty() || bytes.len() > CHUNK_BYTES {
                    return Err(Error::InvalidInput);
                }
                let shard = self.shards.get_mut(&id.shard).ok_or(Error::WrongTransfer)?;
                let Slot::Installing {
                    image,
                    bytes: stored,
                    ..
                } = &mut shard.slot
                else {
                    return Err(Error::WrongPhase);
                };
                if image.movement.id != id {
                    return Err(Error::WrongTransfer);
                }
                let offset = usize::try_from(offset).map_err(|_| Error::ChunkOffset)?;
                let end = offset.checked_add(bytes.len()).ok_or(Error::ChunkOffset)?;
                if end as u64 > image.bytes {
                    return Err(Error::ChunkOffset);
                }
                if offset < stored.len() {
                    if end > stored.len() || stored[offset..end] != bytes {
                        return Err(Error::PayloadMismatch);
                    }
                } else if offset == stored.len() {
                    stored.extend(bytes);
                } else {
                    return Err(Error::ChunkOffset);
                }
                Ok(DataReply::Progress {
                    id,
                    next_offset: stored.len() as u64,
                })
            }
            DataCommand::ResetInstall { image } => {
                let shard = self
                    .shards
                    .get_mut(&image.movement.id.shard)
                    .ok_or(Error::WrongTransfer)?;
                let Slot::Installing {
                    image: old, bytes, ..
                } = &mut shard.slot
                else {
                    return Err(Error::WrongPhase);
                };
                if *old != image {
                    return Err(Error::WrongTransfer);
                }
                bytes.clear();
                Ok(DataReply::Progress {
                    id: image.movement.id,
                    next_offset: 0,
                })
            }
            DataCommand::FinishInstall { id } => {
                let shard = self.shards.get_mut(&id.shard).ok_or(Error::WrongTransfer)?;
                if let Slot::Installed { installation, .. } = &shard.slot {
                    return if installation.image.movement.id == id {
                        Ok(DataReply::Installed(installation.clone()))
                    } else {
                        Err(Error::WrongTransfer)
                    };
                }
                let Slot::Installing {
                    image,
                    bytes,
                    previous_retired,
                } = &shard.slot
                else {
                    return Err(Error::WrongPhase);
                };
                if image.movement.id != id
                    || bytes.len() as u64 != image.bytes
                    || sha256(bytes) != image.sha256
                {
                    return Err(Error::PayloadMismatch);
                }
                let body: ShardImage =
                    serde_json::from_slice(bytes).map_err(|_| Error::PayloadMismatch)?;
                if body.version != 1
                    || body.proof_identity != image.movement
                    || body.fence != image.fence
                    || body.shard_count != self.shard_count
                    || body.data.retention() != image.retention
                    || canonical_bytes(&body, MAX_TRANSFER_BYTES)? != *bytes
                {
                    return Err(Error::PayloadMismatch);
                }
                let installation = InstalledProof {
                    image: image.clone(),
                    installed: origin,
                };
                let previous_retired = previous_retired.clone();
                shard.data = Some(body.data);
                shard.slot = Slot::Installed {
                    installation: installation.clone(),
                    previous_retired,
                };
                Ok(DataReply::Installed(installation))
            }
            DataCommand::Activate { ownership } => {
                if !ownership.valid()
                    || ownership.installation.image.movement.destination != self.group
                {
                    return Err(Error::WrongTransfer);
                }
                let id = &ownership.installation.image.movement.id;
                let shard = self.shards.get_mut(&id.shard).ok_or(Error::WrongTransfer)?;
                if let Slot::Active {
                    activation: Some(old),
                    ..
                } = &shard.slot
                {
                    return if old.ownership == ownership {
                        Ok(DataReply::Activated(old.clone()))
                    } else {
                        Err(Error::WrongTransfer)
                    };
                }
                let Slot::Installed { installation, .. } = &shard.slot else {
                    return Err(Error::WrongPhase);
                };
                if *installation != ownership.installation {
                    return Err(Error::WrongTransfer);
                }
                let activation = ActivationProof {
                    ownership: ownership.clone(),
                    activated: origin,
                };
                shard.slot = Slot::Active {
                    epoch: id.epoch,
                    activation: Some(activation.clone()),
                };
                Ok(DataReply::Activated(activation))
            }
            DataCommand::Cleanup { activation } => {
                if !activation.valid()
                    || activation.ownership.installation.image.movement.source != self.group
                {
                    return Err(Error::WrongTransfer);
                }
                let image = &activation.ownership.installation.image;
                let id = image.movement.id.clone();
                let shard = self.shards.get_mut(&id.shard).ok_or(Error::WrongTransfer)?;
                if let Slot::Retired {
                    activation: old,
                    cleaned,
                } = &shard.slot
                {
                    return if *old == activation {
                        Ok(DataReply::Cleaned {
                            id,
                            origin: *cleaned,
                        })
                    } else {
                        Err(Error::WrongTransfer)
                    };
                }
                if !matches!(&shard.slot,Slot::Fenced{image:old,..} if old==image) {
                    return Err(Error::WrongPhase);
                }
                shard.data = None;
                shard.slot = Slot::Retired {
                    activation,
                    cleaned: origin,
                };
                Ok(DataReply::Cleaned { id, origin })
            }
            DataCommand::Initialize { .. } => unreachable!("handled initialization"),
        }
    }
    fn abort_transfer(&mut self, origin: Origin, abort: AbortProof) -> Result<DataReply, Error> {
        if !abort.valid()
            || !self.movement_valid(&abort.movement)
            || ![abort.movement.source, abort.movement.destination].contains(&self.group)
        {
            return Err(Error::WrongTransfer);
        }
        let key = abort.movement.id.key();
        if let Some(record) = self.aborted.get(&key) {
            return if record.abort == abort {
                Ok(DataReply::Aborted(record.clone()))
            } else {
                Err(Error::PayloadMismatch)
            };
        }
        if self.aborted.len() >= MAX_TRANSFERS {
            return Err(Error::Capacity);
        }
        let shard_id = abort.movement.id.shard;
        if self.group == abort.movement.source {
            let shard = self.shards.get_mut(&shard_id).ok_or(Error::WrongPhase)?;
            match &shard.slot {
                Slot::Active { epoch, .. } if *epoch == abort.movement.previous_epoch => {}
                Slot::Fenced {
                    image,
                    previous_activation,
                } if image.movement == abort.movement => {
                    shard.slot = Slot::Active {
                        epoch: abort.movement.previous_epoch,
                        activation: previous_activation.clone(),
                    };
                }
                _ => return Err(Error::WrongPhase),
            }
            if shard.data.is_none() {
                return Err(Error::WrongPhase);
            }
        } else {
            let previous = match self.shards.get(&shard_id).map(|shard| &shard.slot) {
                None => None,
                Some(Slot::Installing {
                    image,
                    previous_retired,
                    ..
                }) if image.movement == abort.movement => Some(previous_retired.clone()),
                Some(Slot::Installed {
                    installation,
                    previous_retired,
                }) if installation.image.movement == abort.movement => {
                    Some(previous_retired.clone())
                }
                Some(Slot::Retired { activation, .. })
                    if activation.ownership.installation.image.movement.id.epoch
                        < abort.movement.id.epoch =>
                {
                    None
                }
                _ => return Err(Error::WrongPhase),
            };
            if let Some(previous) = previous {
                if let Some(previous) = previous {
                    self.shards.insert(
                        shard_id,
                        Shard {
                            slot: Slot::Retired {
                                activation: previous.activation,
                                cleaned: previous.cleaned,
                            },
                            data: None,
                        },
                    );
                } else {
                    self.shards.remove(&shard_id);
                }
            }
        }
        let record = AbortRecord {
            abort,
            recovered: origin,
        };
        // Leave room for the map key and JSON punctuation as well as the record.
        canonical_bytes(&record, ABORT_RECORD_RESERVE - key.len() - 8)?;
        self.aborted.insert(key, record.clone());
        Ok(DataReply::Aborted(record))
    }

    fn retired_valid(&self, shard: ShardId, previous: &RetiredState, before_epoch: u64) -> bool {
        let image = &previous.activation.ownership.installation.image;
        previous.activation.valid()
            && self.movement_valid(&image.movement)
            && image.movement.source == self.group
            && image.movement.id.shard == shard
            && image.movement.id.epoch < before_epoch
            && previous.cleaned.group == self.group
            && previous.cleaned.index > image.fence.index
            && previous.cleaned.index <= self.applied
    }

    fn movement_valid(&self, movement: &Move) -> bool {
        movement.valid()
            && movement.id.shard < self.shard_count
            && self.groups.contains(&movement.source)
            && self.groups.contains(&movement.destination)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.group == CONTROLLER {
            return Err("data group cannot be controller".into());
        }
        if self.initialized_at.is_none() {
            if self.shard_count != 0
                || !self.groups.is_empty()
                || !self.shards.is_empty()
                || !self.aborted.is_empty()
            {
                return Err("uninitialized data group has state".into());
            }
            return Ok(());
        }
        if self.shard_count == 0
            || usize::from(self.shard_count) > MAX_SHARDS
            || self.groups.is_empty()
            || self.groups.len() > MAX_GROUPS
            || self.groups.contains(&CONTROLLER)
            || !self.groups.contains(&self.group)
            || self.groups.windows(2).any(|p| p[0] >= p[1])
            || self
                .initialized_at
                .is_some_and(|p| p.group != self.group || p.index == 0 || p.index > self.applied)
        {
            return Err("invalid data configuration".into());
        }
        if self.aborted.len() > MAX_TRANSFERS {
            return Err("abort history capacity".into());
        }
        for (key, record) in &self.aborted {
            if *key != record.abort.movement.id.key()
                || !record.abort.valid()
                || !self.movement_valid(&record.abort.movement)
                || ![
                    record.abort.movement.source,
                    record.abort.movement.destination,
                ]
                .contains(&self.group)
                || record.recovered.group != self.group
                || !record.recovered.valid()
                || record.recovered.index > self.applied
                || record.recovered.index <= self.initialized_at.expect("initialized").index
                || canonical_bytes(record, ABORT_RECORD_RESERVE - key.len() - 8).is_err()
            {
                return Err("invalid retained abort tombstone".into());
            }
        }
        let mut sessions = 0usize;
        let mut retries = 0usize;
        let mut payload_bytes = 0usize;
        let mut installing = 0usize;
        let mut reserved_bytes = (MAX_TRANSFERS - self.aborted.len()) * ABORT_RECORD_RESERVE;
        // No-op and rejected commands still advance this watermark. Their
        // decimal growth must never invalidate an otherwise full actor image.
        reserved_bytes += u64::MAX.to_string().len() - self.applied.to_string().len();
        for (id, shard) in &self.shards {
            if *id >= self.shard_count {
                return Err("invalid shard id".into());
            }
            let live_transfer = match &shard.slot {
                Slot::Fenced { image, .. } | Slot::Installing { image, .. } => {
                    Some(&image.movement.id)
                }
                Slot::Installed { installation, .. } => Some(&installation.image.movement.id),
                Slot::Active {
                    activation: Some(activation),
                    ..
                }
                | Slot::Retired { activation, .. } => {
                    Some(&activation.ownership.installation.image.movement.id)
                }
                _ => None,
            };
            if live_transfer.is_some_and(|id| self.aborted.contains_key(&id.key())) {
                return Err("aborted transfer remains in live shard state".into());
            }
            let epoch = match &shard.slot {
                Slot::Active { epoch, activation } => {
                    if *epoch == 0
                        || activation.as_ref().is_some_and(|a| {
                            !a.valid()
                                || a.activated.group != self.group
                                || a.activated.index > self.applied
                                || a.ownership.installation.image.movement.id.shard != *id
                                || a.ownership.installation.image.movement.id.epoch != *epoch
                        })
                    {
                        return Err("invalid active ownership".into());
                    }
                    if *epoch > 1 && activation.is_none() {
                        return Err("active destination has no activation proof".into());
                    }
                    *epoch
                }
                Slot::Fenced {
                    image,
                    previous_activation,
                } => {
                    // Other active shards can keep writing while this one is
                    // fenced. Preserve room for the later cleanup proof.
                    reserved_bytes += 8192;
                    if (image.movement.previous_epoch > 1 && previous_activation.is_none())
                        || previous_activation.as_ref().is_some_and(|activation| {
                            !activation.valid()
                                || activation.activated.group != self.group
                                || activation.activated.index >= image.fence.index
                                || activation.ownership.installation.image.movement.id.shard != *id
                                || activation.ownership.installation.image.movement.id.epoch
                                    != image.movement.previous_epoch
                        })
                    {
                        return Err("invalid pre-fence active provenance".into());
                    }
                    if !image.valid()
                        || !self.movement_valid(&image.movement)
                        || image.fence.group != self.group
                        || image.fence.index > self.applied
                        || image.movement.id.shard != *id
                    {
                        return Err("invalid source fence".into());
                    }
                    image.movement.previous_epoch
                }
                Slot::Installing {
                    image,
                    bytes,
                    previous_retired,
                } => {
                    if previous_retired
                        .as_ref()
                        .is_some_and(|old| !self.retired_valid(*id, old, image.movement.id.epoch))
                    {
                        return Err("invalid pre-install retired provenance".into());
                    }
                    installing += 1;
                    if !image.valid()
                        || !self.movement_valid(&image.movement)
                        || image.movement.destination != self.group
                        || image.movement.id.shard != *id
                        || bytes.len() as u64 > image.bytes
                        || shard.data.is_some()
                    {
                        return Err("invalid partial installation".into());
                    }
                    sessions += image.retention.sessions;
                    retries += image.retention.retry_keys;
                    payload_bytes += image.retention.payload_bytes;
                    // Reserve the complete compact staging representation plus a
                    // conservative allowance for later proof/phase metadata.
                    reserved_bytes += (image.bytes as usize - bytes.len()) * 2 + 8192;
                    continue;
                }
                Slot::Installed {
                    installation,
                    previous_retired,
                } => {
                    // Ownership may already have committed at the controller.
                    // Activation must remain possible even if unrelated writes
                    // fill the rest of this actor's capacity after install.
                    reserved_bytes += 8192;
                    if previous_retired.as_ref().is_some_and(|old| {
                        !self.retired_valid(*id, old, installation.image.movement.id.epoch)
                    }) {
                        return Err("invalid installed retired provenance".into());
                    }
                    installing += 1;
                    if !installation.valid()
                        || !self.movement_valid(&installation.image.movement)
                        || installation.installed.group != self.group
                        || installation.installed.index > self.applied
                        || installation.image.movement.id.shard != *id
                    {
                        return Err("invalid installation".into());
                    }
                    installation.image.movement.previous_epoch
                }
                Slot::Retired {
                    activation,
                    cleaned,
                } => {
                    let image = &activation.ownership.installation.image;
                    if !activation.valid()
                        || !self.movement_valid(&image.movement)
                        || image.movement.source != self.group
                        || image.movement.id.shard != *id
                        || cleaned.group != self.group
                        || cleaned.index > self.applied
                        || cleaned.index <= image.fence.index
                        || shard.data.is_some()
                    {
                        return Err("invalid retired source".into());
                    }
                    continue;
                }
            };
            let data = shard
                .data
                .as_ref()
                .ok_or("serving/fenced shard lacks data")?;
            let frozen = match &shard.slot {
                Slot::Fenced { image, .. } => Some(image),
                Slot::Installed { installation, .. } => Some(&installation.image),
                _ => None,
            };
            if let Some(image) = frozen {
                let body = ShardImage {
                    version: 1,
                    proof_identity: image.movement.clone(),
                    fence: image.fence,
                    shard_count: self.shard_count,
                    data: data.clone(),
                };
                let bytes = canonical_bytes(&body, MAX_TRANSFER_BYTES)
                    .map_err(|_| "frozen image capacity")?;
                if bytes.len() as u64 != image.bytes
                    || sha256(&bytes) != image.sha256
                    || data.retention() != image.retention
                {
                    return Err("frozen data does not match committed image proof".into());
                }
            }
            for (key, value) in &data.cells {
                if key_shard(key, self.shard_count) != Some(*id)
                    || value.as_ref().is_some_and(|v| v.len() > MAX_VALUE_BYTES)
                {
                    return Err("invalid shard cell".into());
                }
            }
            if data.nonces.len() != data.sessions.len() {
                return Err("registration index mismatch".into());
            }
            for (key, session) in &data.sessions {
                sessions += 1;
                if key != &session.id.key()
                    || !session.id.valid()
                    || !self.groups.contains(&session.id.group)
                    || session.nonce.is_empty()
                    || session.nonce.len() > 128
                    || data.nonces.get(&session.nonce) != Some(&session.id)
                    || session.registered.origin != session.id
                {
                    return Err("invalid session identity".into());
                }
                let valid_receipt = |receipt: &Receipt| {
                    receipt.origin.valid()
                        && self.groups.contains(&receipt.origin.group)
                        && receipt.shard == *id
                        && receipt.epoch > 0
                        && receipt.epoch <= epoch
                        && receipt.session == Some(session.id)
                        && (receipt.origin.group != self.group
                            || receipt.origin.index <= self.applied)
                        && frozen.is_none_or(|image| {
                            receipt.origin.group != image.fence.group
                                || receipt.origin.index <= image.fence.index
                        })
                };
                if !valid_receipt(&session.registered)
                    || session.registered.sequence.is_some()
                    || session
                        .closed
                        .as_ref()
                        .is_some_and(|r| !valid_receipt(r) || r.sequence.is_some())
                {
                    return Err("invalid session receipt".into());
                }
                if session.closed.as_ref().is_some_and(|closed| {
                    closed.epoch < session.registered.epoch
                        || (closed.origin.group == session.registered.origin.group
                            && closed.origin.index <= session.registered.origin.index)
                }) {
                    return Err("session close precedes registration".into());
                }
                if session.keys.len() > 256 {
                    return Err("session key capacity".into());
                }
                for (key, cached) in &session.keys {
                    retries += 1;
                    payload_bytes = payload_bytes
                        .checked_add(cached.payload.len())
                        .ok_or("payload overflow")?;
                    if key_shard(key, self.shard_count) != Some(*id)
                        || cached.sequence == 0
                        || cached.digest != sha256(&cached.payload)
                        || !valid_receipt(&cached.receipt)
                        || cached.receipt.sequence != Some(cached.sequence)
                    {
                        return Err("invalid retained retry record".into());
                    }
                    if cached.receipt.epoch < session.registered.epoch
                        || (cached.receipt.origin.group == session.registered.origin.group
                            && cached.receipt.origin.index <= session.registered.origin.index)
                        || session.closed.as_ref().is_some_and(|closed| {
                            cached.receipt.epoch > closed.epoch
                                || (cached.receipt.origin.group == closed.origin.group
                                    && cached.receipt.origin.index >= closed.origin.index)
                        })
                    {
                        return Err("retry receipt violates registration/close ordering".into());
                    }
                    let value: Option<String> = serde_json::from_slice(&cached.payload)
                        .map_err(|_| "invalid retry payload")?;
                    if value.as_ref().is_some_and(|v| v.len() > MAX_VALUE_BYTES)
                        || canonical_bytes(&value, MAX_VALUE_BYTES * 6 + 32)
                            .map_err(|_| "invalid payload")?
                            != cached.payload
                    {
                        return Err("noncanonical retry payload".into());
                    }
                }
            }
        }
        if sessions > MAX_SESSIONS
            || retries > MAX_RETRY_KEYS
            || payload_bytes > MAX_RETAINED_PAYLOAD_BYTES
            || installing > 1
        {
            return Err("actor retention/transfer capacity".into());
        }
        canonical_bytes(self, MAX_ACTOR_BYTES.saturating_sub(reserved_bytes + 4096))
            .map_err(|_| "actor byte capacity".into())
            .map(|_| ())
    }
}
