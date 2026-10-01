use crate::controller::{Route, Transfer};
use crate::data::Slot;
use crate::machine::Machine;
use crate::types::*;
use serde::{Deserialize, Serialize};

// Tagged HTTP enums buffer map keys as strings. Re-enter the JSON deserializer
// at this wire boundary so the core's numeric NodeId endpoint keys retain their
// ordinary JSON map-key semantics; durable core representations stay unchanged.
fn membership_json<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    serde_json::from_value(value).map_err(serde::de::Error::custom)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "query", rename_all = "snake_case", deny_unknown_fields)]
pub enum Query {
    MaintenanceGate,
    Maintenance {
        request_id: String,
    },
    Membership {
        request_id: Option<String>,
    },
    Configuration,
    PendingTransfers,
    Aborted {
        id: TransferId,
    },
    Route {
        shard: ShardId,
    },
    Transfer {
        request_id: String,
    },
    Shard {
        shard: ShardId,
    },
    Read {
        shard: ShardId,
        epoch: u64,
        key: String,
    },
    Export {
        id: TransferId,
        offset: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ShardView {
    Unassigned,
    Active {
        epoch: u64,
        activation: Option<ActivationProof>,
    },
    Fenced {
        image: ImageProof,
    },
    Installing {
        image: ImageProof,
        next_offset: u64,
    },
    Installed {
        installation: InstalledProof,
    },
    Retired {
        activation: ActivationProof,
        cleaned: Origin,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Observed {
    MaintenanceGate {
        generation: Option<Origin>,
        active: Option<MaintenanceTicket>,
        handoff_pending: bool,
    },
    Maintenance {
        ticket: Option<MaintenanceTicket>,
    },
    Membership {
        group: GroupId,
        #[serde(deserialize_with = "membership_json")]
        committed: Box<raft_core::membership::CommittedMembership>,
        #[serde(deserialize_with = "membership_json")]
        effective: Box<raft_core::membership::MembershipState>,
        record: Option<AdminRecord>,
    },
    Configuration {
        group: GroupId,
        initialized_at: Option<Origin>,
        groups: Vec<GroupId>,
        shard_count: u16,
        bootstrap_safe: bool,
    },
    Route {
        route: Route,
        transfer: Option<Transfer>,
    },
    Transfer {
        transfer: Option<Transfer>,
    },
    PendingTransfers {
        transfers: Vec<Transfer>,
    },
    Aborted {
        group: GroupId,
        record: Option<AbortRecord>,
    },
    Shard {
        group: GroupId,
        shard: ShardId,
        ownership: ShardView,
    },
    Value {
        group: GroupId,
        shard: ShardId,
        epoch: u64,
        key: String,
        value: Option<String>,
    },
    Export {
        image: ImageProof,
        offset: u64,
        bytes: Vec<u8>,
    },
}

impl Machine {
    /// Only a group actor that has completed and rechecked ReadIndex calls this.
    pub fn query(&self, query: &Query) -> Result<Observed, Error> {
        match (self, query) {
            (Self::Controller(c), Query::MaintenanceGate) => Ok(Observed::MaintenanceGate {
                generation: c.maintenance_generation(),
                active: c.active_maintenance().cloned(),
                handoff_pending: c.transfers.values().any(|t| !t.complete()),
            }),
            (Self::Controller(c), Query::Maintenance { request_id }) => {
                if !maintenance_request_id_valid(request_id) {
                    return Err(Error::InvalidInput);
                }
                Ok(Observed::Maintenance {
                    ticket: c.maintenance.get(request_id).cloned(),
                })
            }
            (Self::Controller(c), Query::Configuration) => Ok(Observed::Configuration {
                group: CONTROLLER,
                initialized_at: c.initialized_at,
                groups: c.groups.clone(),
                shard_count: c.routes.len() as u16,
                bootstrap_safe: c.transfers.is_empty() && c.maintenance.is_empty(),
            }),
            (Self::Data(d), Query::Configuration) => Ok(Observed::Configuration {
                group: d.group,
                initialized_at: d.initialized_at,
                groups: d.groups.clone(),
                shard_count: d.shard_count,
                bootstrap_safe: d.initialized_at.is_none(),
            }),
            (Self::Controller(c), Query::PendingTransfers) => Ok(Observed::PendingTransfers {
                transfers: c
                    .transfers
                    .values()
                    .filter(|t| !t.complete())
                    .cloned()
                    .collect(),
            }),
            (Self::Controller(c), Query::Route { shard }) => {
                let route = c
                    .routes
                    .get(usize::from(*shard))
                    .ok_or(Error::InvalidInput)?
                    .clone();
                let transfer = route
                    .transfer
                    .as_ref()
                    .and_then(|id| c.transfers.values().find(|t| &t.movement.id == id))
                    .cloned();
                Ok(Observed::Route { route, transfer })
            }
            (Self::Controller(c), Query::Transfer { request_id }) => {
                if request_id.len() > 128 {
                    return Err(Error::InvalidInput);
                }
                Ok(Observed::Transfer {
                    transfer: c.transfers.get(request_id).cloned(),
                })
            }
            (Self::Data(d), Query::Aborted { id }) => {
                if !id.valid() {
                    return Err(Error::InvalidInput);
                }
                Ok(Observed::Aborted {
                    group: d.group,
                    record: d.aborted.get(&id.key()).cloned(),
                })
            }
            (Self::Data(d), Query::Shard { shard }) => {
                if *shard >= d.shard_count {
                    return Err(Error::InvalidInput);
                }
                let ownership = match d.shards.get(shard).map(|s| &s.slot) {
                    None => ShardView::Unassigned,
                    Some(Slot::Active { epoch, activation }) => ShardView::Active {
                        epoch: *epoch,
                        activation: activation.clone(),
                    },
                    Some(Slot::Fenced { image, .. }) => ShardView::Fenced {
                        image: image.clone(),
                    },
                    Some(Slot::Installing { image, bytes, .. }) => ShardView::Installing {
                        image: image.clone(),
                        next_offset: bytes.len() as u64,
                    },
                    Some(Slot::Installed { installation, .. }) => ShardView::Installed {
                        installation: installation.clone(),
                    },
                    Some(Slot::Retired {
                        activation,
                        cleaned,
                    }) => ShardView::Retired {
                        activation: activation.clone(),
                        cleaned: *cleaned,
                    },
                };
                Ok(Observed::Shard {
                    group: d.group,
                    shard: *shard,
                    ownership,
                })
            }
            (Self::Data(d), Query::Read { shard, epoch, key }) => Ok(Observed::Value {
                group: d.group,
                shard: *shard,
                epoch: *epoch,
                key: key.clone(),
                value: d.read(*shard, *epoch, key)?,
            }),
            (Self::Data(d), Query::Export { id, offset }) => {
                let (image, bytes) = d.export(id, *offset)?;
                Ok(Observed::Export {
                    image,
                    offset: *offset,
                    bytes,
                })
            }
            _ => Err(Error::WrongGroup),
        }
    }
}
