use kv_node::transport::{encode_scoped_frame, TransportScope, MAX_FRAME_BYTES};
use raft_core::{Entry, RaftMessage, MAX_APPEND_ENTRIES};
use shard_service::{data::DataCommand, machine::Operation, Error, Origin, MAX_VALUE_BYTES};

#[test]
fn full_append_batch_of_worst_escaped_values_fits_scoped_wire() {
    let operation = Operation::Data(DataCommand::Mutate {
        shard: 0,
        epoch: 1,
        session: Origin { group: 1, index: 1 },
        key: "k".into(),
        sequence: 1,
        value: Some("\0".repeat(MAX_VALUE_BYTES)),
    });
    let command = operation.encode(1).unwrap();
    let message = RaftMessage::AppendEntries {
        term: 2,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: (0..MAX_APPEND_ENTRIES)
            .map(|offset| Entry {
                index: offset as u64 + 1,
                term: 2,
                command: command.clone(),
            })
            .collect(),
        leader_commit: 0,
        contact_round: 1,
    };
    let scope = TransportScope::new("g".repeat(128), (0..=255).collect()).unwrap();
    let encoded = encode_scoped_frame(1, &message, &scope).unwrap();
    assert!(encoded.len() <= MAX_FRAME_BYTES as usize + 4);
    assert!(
        encoded.len() > 7 * 1024 * 1024,
        "fixture must exercise near-frame-sized escaped payloads"
    );
}

#[test]
fn oversized_malformed_fields_are_rejected_before_raft_admission() {
    let operation = Operation::Data(DataCommand::Register {
        shard: 0,
        epoch: 1,
        nonce: "\"".repeat(200 * 1024),
    });
    assert_eq!(operation.encode(1), Err(Error::Capacity));
}
