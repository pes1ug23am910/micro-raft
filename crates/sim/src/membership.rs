//! Independent configuration witness. This deliberately does not call the
//! production membership replay or quorum helpers.
use crate::*;
use raft_core::membership::{AdminOperation, ConfigurationPhase};

pub(crate) fn required_quorums(
    genesis: &[NodeId],
    history: &[Entry],
) -> BTreeMap<LogIndex, (BTreeSet<NodeId>, Option<BTreeSet<NodeId>>)> {
    let mut old: BTreeSet<_> = genesis.iter().copied().collect();
    let mut new: Option<BTreeSet<NodeId>> = None;
    let mut obligations = BTreeMap::new();
    for entry in history {
        if let Command::Configuration(change) = &entry.command {
            match change.phase {
                ConfigurationPhase::Apply => {}
                ConfigurationPhase::Joint => {
                    assert!(
                        new.is_none(),
                        "witness saw overlapping joint configurations"
                    );
                    new = Some(match &change.operation {
                        AdminOperation::SetVoters { voters } => voters.iter().copied().collect(),
                        AdminOperation::Remove { id } => {
                            old.iter().copied().filter(|voter| voter != id).collect()
                        }
                        AdminOperation::AddLearner { .. } => {
                            panic!("learner cannot require joint quorum")
                        }
                    });
                }
                ConfigurationPhase::Final => {
                    // Joint must have committed before final was appended;
                    // final itself uses the newly effective stable quorum.
                    old = new
                        .take()
                        .expect("witness saw final without joint configuration");
                }
            }
        }
        obligations.insert(entry.index, (old.clone(), new.clone()));
    }
    obligations
}

pub(crate) fn witnessed_quorum(
    old: &BTreeSet<NodeId>,
    new: Option<&BTreeSet<NodeId>>,
    durable: &BTreeSet<NodeId>,
) -> bool {
    let majority =
        |members: &BTreeSet<NodeId>| members.intersection(durable).count() > members.len() / 2;
    majority(old) && new.is_none_or(majority)
}

impl Sim {
    /// All actors share a genesis; actors outside it are initially nonvoting
    /// joiners. Only replicated admission can make them members.
    pub fn configure_group(&mut self, group_id: &str, genesis: Vec<NodeId>) {
        assert_eq!(self.now_ms, 0);
        assert!(self.committed.is_empty());
        self.genesis_voters = genesis.clone();
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for id in ids {
            let effects = self
                .nodes
                .get_mut(&id)
                .expect("actor")
                .core
                .initialize_group(group_id.to_owned(), genesis.clone())
                .expect("fresh group");
            self.execute_effects(id, effects);
        }
        self.check_all_invariants();
    }

    pub fn membership_change(
        &mut self,
        id: NodeId,
        request_id: &str,
        operation: AdminOperation,
    ) -> Vec<Effect> {
        self.read_input(
            id,
            Input::MembershipChange {
                request_id: request_id.to_owned(),
                operation,
            },
        )
    }
}
