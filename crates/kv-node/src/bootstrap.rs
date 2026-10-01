//! Ordered pre-service identity, consensus, and derived-application recovery.
use crate::{
    application::StateMachine,
    applied_store::{ApplicationIdentity, AppliedStore, StateBackend},
    config::ValidatedConfig,
    durability,
    snapshot::SnapshotImage,
    storage::Storage,
};
use raft_core::{Effect, HardState, RaftNode};
use serde::{Deserialize, Serialize};
use std::{io, path::Path};

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapIntent {
    version: u32,
    identity: ApplicationIdentity,
    backend: StateBackend,
}
const INTENT: &str = "group-bootstrap-intent.json";
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn exists_state(dir: &Path) -> io::Result<bool> {
    [
        "hardstate.json",
        "log.jsonl",
        "CURRENT",
        "application-identity.json",
        "state-lsm",
        "state-redb",
    ]
    .iter()
    .try_fold(false, |found, name| {
        dir.join(name).try_exists().map(|exists| found || exists)
    })
}

pub struct Bootstrapped {
    pub node: RaftNode,
    pub storage: Storage,
    pub image: Option<SnapshotImage>,
    pub application_store: AppliedStore,
    pub application: StateMachine,
}

/// No listener or tick may precede this transaction. A bootstrap intent is only
/// permission to finish an exact index-zero initialization, never group authority.
pub fn recover(cfg: &ValidatedConfig, seed: u64) -> io::Result<Bootstrapped> {
    let existed = exists_state(&cfg.data_dir)?;
    let (mut storage, recovered) = Storage::open_recovered(&cfg.data_dir)?;
    let image = recovered.snapshot;
    let hard = recovered.hard_state;
    let entries = recovered.entries;
    let intent = match std::fs::read(cfg.data_dir.join(INTENT)) {
        Ok(bytes) => Some(serde_json::from_slice::<BootstrapIntent>(&bytes)?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let mut fresh = !existed;
    if let Some(intent) = &intent {
        if intent.version != 1
            || cfg.group_id.as_ref() != Some(&intent.identity.group)
            || cfg.genesis_voters != intent.identity.genesis_voters
            || cfg.state_backend != intent.backend
            || image.is_some()
            || !entries.is_empty()
            || hard.current_term != 0
            || hard.voted_for.is_some()
            || hard.membership.as_ref().is_some_and(|m| {
                m.index != 0
                    || m.state.group_id != intent.identity.group
                    || m.state.genesis_voters != intent.identity.genesis_voters
            })
        {
            return Err(invalid(
                "bootstrap intent cannot resume a different or already active group",
            ));
        }
        fresh = true;
    }
    let peers = cfg.peers.iter().map(|peer| peer.id).collect();
    let descriptor = image.as_ref().map(|image| image.descriptor().clone());
    let mut node = RaftNode::restore_with_snapshot(cfg.id, peers, seed, hard, descriptor, entries)
        .map_err(|error| invalid(error.to_string()))?;
    let mut identity_effects = Vec::new();
    match &cfg.group_id {
        Some(group) => {
            if node.hard.membership.is_none() && !fresh && !cfg.migrate_legacy_group {
                return Err(invalid(
                    "legacy directory adoption requires --migrate-legacy-group",
                ));
            }
            if cfg.migrate_legacy_group && !node.committed_membership().state.records.is_empty() {
                return Err(invalid(
                    "legacy migration is unavailable after membership administration",
                ));
            }
            identity_effects = node
                .initialize_group(group.clone(), cfg.genesis_voters.clone())
                .map_err(invalid)?;
        }
        None => {
            if node
                .hard
                .membership
                .as_ref()
                .is_some_and(|m| m.state.group_id != raft_core::membership::LEGACY_GROUP_ID)
            {
                return Err(invalid(
                    "persisted explicit group requires matching --group-id and --genesis-voters",
                ));
            }
        }
    }
    let identity = ApplicationIdentity::legacy(&node);
    crate::applied_store::preflight_identity(
        &cfg.data_dir,
        cfg.state_backend,
        &identity,
        fresh,
        cfg.migrate_legacy_group,
    )?;
    if fresh && cfg.group_id.is_some() && intent.is_none() {
        let intent = BootstrapIntent {
            version: 1,
            identity: identity.clone(),
            backend: cfg.state_backend,
        };
        durability::publish_file(
            &cfg.data_dir,
            "group-bootstrap-intent.json.new",
            INTENT,
            &serde_json::to_vec(&intent)?,
        )?;
    }
    // Includes selected-snapshot authority refresh; term and vote are preserved.
    for effect in identity_effects {
        match effect {
            Effect::PersistHardState(hard) => storage.save_hard_state(&hard)?,
            _ => return Err(invalid("unexpected group initialization effect")),
        }
    }
    if node.hard != HardState::default() || node.recovery_required() {
        storage.save_hard_state(&node.hard)?;
    }
    let (mut application_store, mut application) = AppliedStore::open_for_group(
        &cfg.data_dir,
        cfg.state_backend,
        identity,
        &mut node,
        image.as_ref(),
        fresh,
        cfg.migrate_legacy_group,
    )?;
    // Recover known committed configuration prefixes before exposing HTTP. The
    // engine watermark was restored above, so this never reapplies visible rows.
    for effect in node.step(raft_core::Input::Recover) {
        match effect {
            Effect::PersistHardState(hard) => storage.save_hard_state(&hard)?,
            Effect::Apply(entry) => {
                application
                    .apply(&entry)
                    .map_err(|error| invalid(error.to_string()))?;
                let cells = application_store.cells(&application, &entry)?;
                application_store.commit(entry.index, entry.index, entry.term, &cells)?;
            }
            Effect::MembershipChanged { .. } => {}
            _ => return Err(invalid("unexpected recovery effect before service")),
        }
    }
    if application.last_applied() != node.last_applied {
        return Err(invalid("recovery application watermark mismatch"));
    }
    if cfg.data_dir.join(INTENT).try_exists()? {
        std::fs::remove_file(cfg.data_dir.join(INTENT))?;
        durability::sync_directory(&cfg.data_dir)?;
    }
    Ok(Bootstrapped {
        node,
        storage,
        image,
        application_store,
        application,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use raft_core::{Command, Entry, SnapshotMetadata};
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "micro-raft-membership-bootstrap-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn config(
        dir: &Path,
        backend: StateBackend,
        explicit: bool,
        migration: bool,
    ) -> ValidatedConfig {
        let mut args = vec![
            "kv-node".to_owned(),
            "--id".into(),
            "1".into(),
            "--raft-port".into(),
            "19001".into(),
            "--http-port".into(),
            "18001".into(),
            "--peers".into(),
            "2@127.0.0.1:19002,3@127.0.0.1:19003".into(),
            "--data-dir".into(),
            dir.to_string_lossy().into_owned(),
            "--state-backend".into(),
            backend.as_str().into(),
        ];
        if explicit {
            args.extend([
                "--group-id".into(),
                "migration-test".into(),
                "--genesis-voters".into(),
                "1,2,3".into(),
            ]);
        }
        if migration {
            args.push("--migrate-legacy-group".into());
        }
        crate::config::validate(&crate::config::Config::try_parse_from(args).unwrap()).unwrap()
    }
    fn legacy(dir: &Path, backend: StateBackend) {
        let cfg = config(dir, backend, false, false);
        let mut boot = recover(&cfg, 7).unwrap();
        boot.node.hard.current_term = 1;
        boot.storage.save_hard_state(&boot.node.hard).unwrap();
        let entries = vec![
            Entry {
                index: 1,
                term: 1,
                command: Command::NoOp,
            },
            Entry {
                index: 2,
                term: 1,
                command: Command::Put {
                    key: "kept".into(),
                    value: "value".into(),
                },
            },
        ];
        boot.storage.append_entries(None, &entries).unwrap();
        for entry in &entries {
            boot.application.apply(entry).unwrap();
            let cells = boot
                .application_store
                .cells(&boot.application, entry)
                .unwrap();
            boot.application_store
                .commit(entry.index, entry.index, entry.term, &cells)
                .unwrap();
        }
        let image = SnapshotImage::new(
            SnapshotMetadata {
                last_included_index: 2,
                last_included_term: 1,
                members: vec![1, 2, 3],
                membership: None,
            },
            &boot.application,
        )
        .unwrap();
        assert_eq!(&image.bytes()[..8], b"MRFTSN01");
        boot.storage.publish_snapshot(&image, &[]).unwrap();
    }
    #[test]
    fn explicit_migration_preserves_engine_and_snapshot_and_rejects_silent_or_wrong_identity() {
        for backend in [StateBackend::Memory, StateBackend::Lsm, StateBackend::Redb] {
            let dir = Directory::new();
            legacy(&dir.0, backend);
            let before = std::fs::read(dir.0.join("hardstate.json")).unwrap();
            assert!(recover(&config(&dir.0, backend, true, false), 7).is_err());
            assert_eq!(std::fs::read(dir.0.join("hardstate.json")).unwrap(), before);
            let boot = recover(&config(&dir.0, backend, true, true), 8).unwrap();
            assert_eq!(
                boot.application.values().get("kept").map(String::as_str),
                Some("value")
            );
            assert_eq!(boot.node.last_applied, 2);
            assert_eq!(
                boot.node.committed_membership().state.group_id,
                "migration-test"
            );
            assert!(
                boot.image
                    .as_ref()
                    .unwrap()
                    .descriptor()
                    .metadata
                    .membership
                    .is_none(),
                "V1 remains readable under explicit migration"
            );
            drop(boot);
            let bytes = std::fs::read(dir.0.join("hardstate.json")).unwrap();
            #[derive(Deserialize)]
            struct OldHard {
                #[serde(rename = "current_term")]
                _term: u64,
                #[serde(rename = "voted_for")]
                _vote: Option<u8>,
            }
            assert!(
                serde_json::from_slice::<OldHard>(&bytes).is_err(),
                "old binary must refuse outer version2"
            );
            let mut wrong = config(&dir.0, backend, true, false);
            wrong.group_id = Some("other-group".into());
            assert!(recover(&wrong, 9).is_err());
            assert_eq!(std::fs::read(dir.0.join("hardstate.json")).unwrap(), bytes);
            let boot = recover(&config(&dir.0, backend, true, false), 10).unwrap();
            assert_eq!(boot.application.last_applied(), 2);
        }
    }
    #[test]
    fn migration_crashes_after_core_and_after_engine_resume_only_exact_explicit_request() {
        for backend in [StateBackend::Lsm, StateBackend::Redb] {
            let dir = Directory::new();
            legacy(&dir.0, backend);
            let old_binding = std::fs::read(dir.0.join("application-identity.json")).unwrap();
            let mut boot = recover(&config(&dir.0, backend, false, false), 7).unwrap();
            for effect in boot
                .node
                .initialize_group("migration-test".into(), vec![1, 2, 3])
                .unwrap()
            {
                let Effect::PersistHardState(hard) = effect else {
                    panic!("unexpected effect")
                };
                boot.storage.save_hard_state(&hard).unwrap();
            }
            drop(boot); // Reached disk state: new core identity, old engine and binding.
            assert!(recover(&config(&dir.0, backend, true, false), 8).is_err());
            let migrated = recover(&config(&dir.0, backend, true, true), 9).unwrap();
            assert_eq!(
                migrated
                    .application
                    .values()
                    .get("kept")
                    .map(String::as_str),
                Some("value")
            );
            drop(migrated);
            // Exact independently constructed next crash boundary: the engine's
            // atomic metadata rewrite survived, its matching marker did not.
            durability::publish_file(
                &dir.0,
                "application-identity.json.new",
                "application-identity.json",
                &old_binding,
            )
            .unwrap();
            assert!(recover(&config(&dir.0, backend, true, false), 10).is_err());
            let resumed = recover(&config(&dir.0, backend, true, true), 11).unwrap();
            assert_eq!(resumed.application.last_applied(), 2);
            assert_eq!(
                resumed.application.values().get("kept").map(String::as_str),
                Some("value")
            );
        }
    }
    #[test]
    fn selected_v2_snapshot_refreshes_older_hard_membership_before_engine_or_service() {
        use raft_core::membership::{
            AdminOperation, ConfigurationEntry, ConfigurationPhase, MemberEndpoints,
        };
        for backend in [StateBackend::Memory, StateBackend::Lsm, StateBackend::Redb] {
            let dir = Directory::new();
            legacy(&dir.0, backend);
            let cfg = config(&dir.0, backend, true, true);
            let mut boot = recover(&cfg, 7).unwrap();
            let change = ConfigurationEntry {
                request_id: "add4".into(),
                operation: AdminOperation::AddLearner {
                    id: 4,
                    endpoints: MemberEndpoints {
                        raft: "127.0.0.1:19004".into(),
                        http: "127.0.0.1:18004".into(),
                    },
                },
                phase: ConfigurationPhase::Apply,
            };
            let entry = Entry {
                index: 3,
                term: 1,
                command: Command::Configuration(change.clone()),
            };
            boot.storage
                .append_entries(None, std::slice::from_ref(&entry))
                .unwrap();
            boot.application.apply(&entry).unwrap();
            let membership = boot
                .node
                .committed_membership()
                .advanced(3, 1, &change)
                .unwrap();
            let image = SnapshotImage::new(
                SnapshotMetadata {
                    last_included_index: 3,
                    last_included_term: 1,
                    members: vec![1, 2, 3, 4],
                    membership: Some(membership.clone()),
                },
                &boot.application,
            )
            .unwrap();
            assert_eq!(&image.bytes()[..8], b"MRFTSN02");
            boot.storage.publish_snapshot(&image, &[]).unwrap();
            // CURRENT now selects the new membership/application, but the old
            // hard state and engine still describe the earlier boundary.
            drop(boot);
            let mut cfg = cfg;
            cfg.migrate_legacy_group = false;
            let reopened = recover(&cfg, 8).unwrap();
            assert_eq!(reopened.node.committed_membership(), &membership);
            assert_eq!(reopened.node.last_applied, 3);
            assert_eq!(reopened.application.last_applied(), 3);
            assert!(!reopened.node.recovery_required());
            drop(reopened);
            let (_, durable) = Storage::open_recovered(&dir.0).unwrap();
            assert_eq!(durable.hard_state.membership, Some(membership));
        }
    }
    #[test]
    fn interrupted_fresh_group_initialization_requires_exact_zero_boundary_intent() {
        let dir = Directory::new();
        let cfg = config(&dir.0, StateBackend::Lsm, true, false);
        let identity = ApplicationIdentity {
            group: "migration-test".into(),
            genesis_voters: vec![1, 2, 3],
        };
        let intent = BootstrapIntent {
            version: 1,
            identity,
            backend: StateBackend::Lsm,
        };
        durability::publish_file(
            &dir.0,
            "group-bootstrap-intent.json.new",
            INTENT,
            &serde_json::to_vec(&intent).unwrap(),
        )
        .unwrap();
        let (mut store, _, _) = Storage::open(&dir.0).unwrap();
        let (node, _) =
            RaftNode::new_for_group(1, vec![1, 2, 3], 7, "migration-test".into()).unwrap();
        store.save_hard_state(&node.hard).unwrap();
        drop(store);
        let mut wrong = config(&dir.0, StateBackend::Redb, true, false);
        assert!(recover(&wrong, 8).is_err());
        wrong = cfg;
        let boot = recover(&wrong, 9).unwrap();
        assert_eq!(boot.application.last_applied(), 0);
        assert!(!dir.0.join(INTENT).exists());
    }
}
