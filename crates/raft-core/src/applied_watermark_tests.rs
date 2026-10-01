use crate::*;

fn restored() -> RaftNode {
    let hard = HardState {
        current_term: 2,
        ..HardState::default()
    };
    let log = vec![1, 1, 2]
        .into_iter()
        .enumerate()
        .map(|(offset, term)| Entry {
            index: offset as u64 + 1,
            term,
            command: Command::NoOp,
        })
        .collect();
    RaftNode::restore(1, vec![2, 3], 7, hard, log).unwrap()
}

#[test]
fn durable_applied_watermark_skips_already_visible_entries() {
    let mut node = restored();
    node.restore_applied_watermark(2, 1).unwrap();
    assert_eq!((node.commit_index, node.last_applied), (2, 2));
    let mut effects = Vec::new();
    node.commit_index = 3;
    node.apply_committed(&mut effects);
    assert_eq!(
        effects,
        vec![Effect::Apply(Entry {
            index: 3,
            term: 2,
            command: Command::NoOp
        })]
    );
}

#[test]
fn wrong_engine_term_range_regression_or_late_initialization_refuses() {
    let mut node = restored();
    assert!(node.restore_applied_watermark(4, 2).is_err());
    assert!(node.restore_applied_watermark(2, 2).is_err());
    assert_eq!((node.commit_index, node.last_applied), (0, 0));
    node.restore_applied_watermark(2, 1).unwrap();
    assert!(node.restore_applied_watermark(1, 1).is_err());
    node.step(Input::Tick { now_ms: 1 });
    assert!(node.restore_applied_watermark(3, 2).is_err());
}

#[test]
fn later_known_commit_is_preserved_and_only_unapplied_suffix_is_emitted() {
    let mut node = restored();
    node.commit_index = 3;
    node.restore_applied_watermark(2, 1).unwrap();
    assert_eq!((node.commit_index, node.last_applied), (3, 2));
    let mut effects = Vec::new();
    node.apply_committed(&mut effects);
    assert_eq!(effects.len(), 1);
    assert!(matches!(effects[0], Effect::Apply(Entry { index: 3, .. })));
}
