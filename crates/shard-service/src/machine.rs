use crate::controller::{Controller, ControllerCommand, ControllerReply};
use crate::data::{DataCommand, DataMachine, DataReply};
use crate::types::*;
use serde::{Deserialize, Serialize};

pub const ENVELOPE_KEY: &str = "@micro-raft/shard-service/v1";
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "state",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Machine {
    Controller(Controller),
    Data(DataMachine),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "command",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Operation {
    Controller(ControllerCommand),
    Data(DataCommand),
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    group: GroupId,
    operation: Operation,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "reply",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Reply {
    Controller(ControllerReply),
    Data(DataReply),
    Rejected(Error),
    NoOp,
}

impl Machine {
    pub fn new(group: GroupId) -> Result<Self, Error> {
        if group == CONTROLLER {
            Ok(Self::Controller(Controller::default()))
        } else {
            Ok(Self::Data(DataMachine::new(group)?))
        }
    }
    pub fn group(&self) -> GroupId {
        match self {
            Self::Controller(_) => CONTROLLER,
            Self::Data(data) => data.group,
        }
    }
    pub fn applied(&self) -> u64 {
        match self {
            Self::Controller(c) => c.applied,
            Self::Data(d) => d.applied,
        }
    }
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Controller(c) => c.validate(),
            Self::Data(d) => d.validate(),
        }
    }
    pub fn operation(&mut self, index: u64, operation: Operation) -> Result<Reply, String> {
        match (self, operation) {
            (Self::Controller(c), Operation::Controller(cmd)) => Ok(match c.apply(index, cmd)? {
                Ok(r) => Reply::Controller(r),
                Err(e) => Reply::Rejected(e),
            }),
            (Self::Data(d), Operation::Data(cmd)) => Ok(match d.apply(index, cmd)? {
                Ok(r) => Reply::Data(r),
                Err(e) => Reply::Rejected(e),
            }),
            _ => Err("command targets a different machine kind".into()),
        }
    }
    pub fn apply(&mut self, entry: &raft_core::Entry) -> Result<Reply, String> {
        match &entry.command {
            raft_core::Command::NoOp | raft_core::Command::Configuration(_) => {
                match self {
                    Self::Controller(c) => c.advance(entry.index)?,
                    Self::Data(d) => d.advance(entry.index)?,
                };
                Ok(Reply::NoOp)
            }
            raft_core::Command::Put { key, value } if key == ENVELOPE_KEY => {
                let envelope: Envelope = serde_json::from_str(value).map_err(|e| e.to_string())?;
                if envelope.version != 1 || envelope.group != self.group() {
                    return Err("log envelope group/version mismatch".into());
                }
                self.operation(entry.index, envelope.operation)
            }
            _ => Err("non-shard command in shard actor log".into()),
        }
    }
}
impl Operation {
    pub fn encode(&self, group: GroupId) -> Result<raft_core::Command, Error> {
        // Worst-case JSON byte arrays and escaped values still fit the Raft
        // transport's frame bound; admission rejects before log mutation.
        if !self.targets(group) {
            return Err(Error::WrongGroup);
        }
        let bytes = canonical_bytes(
            &Envelope {
                version: 1,
                group,
                operation: self.clone(),
            },
            512 * 1024,
        )?;
        let value = String::from_utf8(bytes).map_err(|_| Error::InvalidInput)?;
        let command = raft_core::Command::Put {
            key: ENVELOPE_KEY.into(),
            value,
        };
        // Bound the final escaped Command representation, not merely the inner
        // envelope. Sixteen maximal commands plus entry/scope metadata fit 8 MiB.
        canonical_bytes(&command, 480 * 1024)?;
        Ok(command)
    }
    pub fn targets(&self, group: GroupId) -> bool {
        matches!((self, group), (Self::Controller(_), CONTROLLER))
            || matches!(self, Self::Data(_)) && group != CONTROLLER
    }
}
