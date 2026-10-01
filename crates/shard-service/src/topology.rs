//! Immutable bootstrap identity and exclusive ownership of a service directory.
use crate::types::*;
use kv_node::durability;
use kv_node::transport::TransportScope;
use raft_core::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub id: NodeId,
    pub http: SocketAddr,
    pub raft: BTreeMap<GroupId, SocketAddr>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    pub version: u32,
    pub cluster: String,
    pub groups: Vec<GroupId>,
    pub owners: Vec<GroupId>,
    pub genesis_voters: BTreeMap<GroupId, Vec<NodeId>>,
    pub nodes: Vec<Member>,
}
#[derive(Serialize)]
struct Bootstrap<'a> {
    version: u32,
    cluster: &'a str,
    groups: &'a [GroupId],
    owners: &'a [GroupId],
    genesis_voters: &'a BTreeMap<GroupId, Vec<NodeId>>,
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
impl Topology {
    pub fn validate(&self) -> io::Result<()> {
        if self.version != 1
            || self.cluster.is_empty()
            || self.cluster.len() > 32
            || !self
                .cluster
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
            || self.groups.is_empty()
            || self.groups.len() > MAX_GROUPS
            || self.groups.contains(&CONTROLLER)
            || self.groups.windows(2).any(|p| p[0] >= p[1])
            || self.owners.is_empty()
            || self.owners.len() > MAX_SHARDS
            || self.owners.iter().any(|g| !self.groups.contains(g))
            || self.nodes.is_empty()
            || self.nodes.len() > 15
            || self.nodes.windows(2).any(|p| p[0].id >= p[1].id)
        {
            return Err(invalid("invalid immutable service topology"));
        }
        let groups: BTreeSet<_> = self.all_groups().collect();
        if self.genesis_voters.keys().copied().collect::<BTreeSet<_>>() != groups
            || self.genesis_voters.values().any(|voters| {
                voters.is_empty() || voters.len() > 15 || voters.windows(2).any(|v| v[0] >= v[1])
            })
        {
            return Err(invalid(
                "each group needs explicit sorted immutable genesis voters",
            ));
        }
        let mut addresses = BTreeSet::new();
        for node in &self.nodes {
            if node.raft.keys().copied().collect::<BTreeSet<_>>() != groups {
                return Err(invalid(
                    "every node must host exactly the configured groups",
                ));
            }
            for addr in std::iter::once(&node.http).chain(node.raft.values()) {
                let ip = match addr.ip() {
                    IpAddr::V6(ip) => ip
                        .to_ipv4_mapped()
                        .map(IpAddr::V4)
                        .unwrap_or(IpAddr::V6(ip)),
                    ip => ip,
                };
                let canonical = SocketAddr::new(ip, addr.port());
                if addr.port() == 0
                    || ip.is_unspecified()
                    || ip.is_multicast()
                    || matches!(ip,IpAddr::V4(ip) if ip.is_broadcast())
                    || matches!(addr,SocketAddr::V6(addr) if addr.scope_id()!=0 || addr.flowinfo()!=0)
                    || !addresses.insert(canonical)
                {
                    return Err(invalid("unreachable or duplicate service address"));
                }
            }
        }
        Ok(())
    }
    pub fn all_groups(&self) -> impl Iterator<Item = GroupId> + '_ {
        std::iter::once(CONTROLLER).chain(self.groups.iter().copied())
    }
    fn bootstrap(&self) -> Bootstrap<'_> {
        Bootstrap {
            version: self.version,
            cluster: &self.cluster,
            groups: &self.groups,
            owners: &self.owners,
            genesis_voters: &self.genesis_voters,
        }
    }
    pub fn fingerprint(&self) -> String {
        sha256(&serde_json::to_vec(&self.bootstrap()).expect("serializable bootstrap identity"))
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
    pub fn scope(&self, group: GroupId) -> io::Result<TransportScope> {
        if !self.all_groups().any(|g| g == group) {
            return Err(invalid("unknown topology group"));
        }
        TransportScope::new(
            format!("{}/g{group}/{}", self.cluster, self.fingerprint()),
            self.genesis_voters[&group].clone(),
        )
    }
}

pub struct DirectoryGuard {
    _lock: File,
    fresh: bool,
    pub boot: u64,
}
#[derive(Serialize)]
struct Identity<'a> {
    version: u32,
    node: NodeId,
    bootstrap: Bootstrap<'a>,
}
impl DirectoryGuard {
    pub fn open(path: &Path, node: NodeId, topology: &Topology) -> io::Result<Self> {
        topology.validate()?;
        if !topology.nodes.iter().any(|n| n.id == node) {
            return Err(invalid("node not in immutable topology"));
        }
        durability::create_dir_all(path)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.join("LOCK"))?;
        lock.try_lock().map_err(io::Error::other)?;
        let identity = serde_json::to_vec(&Identity {
            version: 1,
            node,
            bootstrap: topology.bootstrap(),
        })
        .map_err(io::Error::other)?;
        let marker = path.join("IDENTITY");
        let fresh = !marker.try_exists()?;
        if fresh {
            if fs::read_dir(path)?.any(|entry| entry.map_or(true, |e| e.file_name() != "LOCK")) {
                return Err(invalid("unidentified nonempty service directory"));
            }
            publish(path, "IDENTITY", &identity)?;
            for group in topology.all_groups() {
                let dir = path.join(format!("group-{group}"));
                durability::create_dir_all(&dir)?;
                publish(
                    &dir,
                    "GROUP",
                    format!("{}:{group}\n", topology.fingerprint()).as_bytes(),
                )?;
                let log = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(dir.join("log.jsonl"))?;
                log.sync_all()?;
                drop(log);
                durability::sync_directory(&dir)?;
                let (mut storage, recovered) = kv_node::storage::Storage::open_recovered(&dir)?;
                storage.save_hard_state(&recovered.hard_state)?;
            }
        } else {
            if fs::read(&marker)? != identity {
                return Err(invalid(
                    "service directory identity differs from node/topology",
                ));
            }
            if fs::read(path.join("READY"))? != b"shard-service-v1\n" {
                return Err(invalid("incomplete service initialization"));
            }
            for group in topology.all_groups() {
                let dir = path.join(format!("group-{group}"));
                if fs::read(dir.join("GROUP"))?
                    != format!("{}:{group}\n", topology.fingerprint()).as_bytes()
                {
                    return Err(invalid("group directory identity mismatch"));
                }
                // A missing selected manifest or legacy WAL must never silently
                // turn an initialized group into a fresh empty state machine.
                if !dir.is_dir()
                    || (!dir.join("CURRENT").is_file() && !dir.join("log.jsonl").is_file())
                    || !dir.join("hardstate.json").is_file()
                {
                    return Err(invalid("initialized group storage payload missing"));
                }
            }
        }
        let boot = if fresh {
            1
        } else {
            let bytes = fs::read(path.join("BOOT"))?;
            let previous: u64 = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
            if previous == 0 {
                return Err(invalid("invalid boot incarnation counter"));
            }
            previous
                .checked_add(1)
                .ok_or_else(|| invalid("boot incarnation counter exhausted"))?
        };
        if !fresh {
            let stage = path.join("BOOT.new");
            match fs::symlink_metadata(&stage) {
                Ok(metadata) if metadata.file_type().is_file() => {
                    fs::remove_file(stage)?;
                    durability::sync_directory(path)?;
                }
                Ok(_) => {
                    return Err(invalid(
                        "unpublished boot stage is not a regular owned file",
                    ))
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        publish(path, "BOOT", boot.to_string().as_bytes())?;
        Ok(Self {
            _lock: lock,
            fresh,
            boot,
        })
    }
    pub fn mark_ready(&self, path: &Path) -> io::Result<()> {
        if self.fresh {
            publish(path, "READY", b"shard-service-v1\n")?;
        }
        Ok(())
    }
}
fn publish(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let staged = dir.join(format!("{name}.new"));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&staged)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(staged, dir.join(name))?;
    durability::sync_directory(dir)
}
