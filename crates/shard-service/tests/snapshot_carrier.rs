use kv_node::application::{ApplicationSnapshot, StateMachine};
use kv_node::snapshot::{SnapshotImage, SnapshotMetadata};
use shard_service::data::{DataCommand as D, DataMachine, DataReply};
use shard_service::machine::Machine;
use shard_service::{canonical_bytes, snapshot, MAX_ACTOR_BYTES};

fn populated() -> Machine {
    let mut data = DataMachine::new(1).unwrap();
    data.apply(
        1,
        D::Initialize {
            shard_count: 1,
            groups: vec![1, 2],
            owned: vec![0],
        },
    )
    .unwrap()
    .unwrap();
    let DataReply::Receipt(receipt) = data
        .apply(
            2,
            D::Register {
                shard: 0,
                epoch: 1,
                nonce: "client".into(),
            },
        )
        .unwrap()
        .unwrap()
    else {
        panic!()
    };
    data.apply(
        3,
        D::Mutate {
            shard: 0,
            epoch: 1,
            session: receipt.session.unwrap(),
            key: "large".into(),
            sequence: 1,
            value: Some("é\\\"".repeat(10_000)),
        },
    )
    .unwrap()
    .unwrap();
    Machine::Data(data)
}
fn metadata() -> SnapshotMetadata {
    SnapshotMetadata {
        membership: None,
        last_included_index: 3,
        last_included_term: 2,
        members: vec![1, 2, 3],
    }
}
fn alter(image: SnapshotImage, change: impl FnOnce(&mut ApplicationSnapshot)) -> SnapshotImage {
    let mut carrier = image.application().export_snapshot();
    change(&mut carrier);
    let app = StateMachine::import_snapshot(carrier, 3).unwrap();
    SnapshotImage::new(metadata(), &app).unwrap()
}

#[test]
fn carrier_chunks_large_unicode_payload_and_restores_exact_actor() {
    let machine = populated();
    let image = snapshot::capture(metadata(), &machine).unwrap();
    assert!(image.application().values().len() > 3);
    assert!(image
        .application()
        .values()
        .values()
        .all(|value| value.len() <= 64 * 1024));
    let decoded = SnapshotImage::decode(image.bytes().to_vec()).unwrap();
    assert_eq!(snapshot::restore(&decoded, 1).unwrap(), machine);
    assert!(snapshot::restore(&decoded, 2).is_err());
    assert!(canonical_bytes(&machine, MAX_ACTOR_BYTES).is_ok());
}

#[test]
fn valid_outer_snapshot_cannot_hide_missing_extra_or_changed_carrier_data() {
    for mutation in 0..3 {
        let image = snapshot::capture(metadata(), &populated()).unwrap();
        let altered = alter(image, |carrier| match mutation {
            0 => {
                carrier.values.remove(0);
            }
            1 => carrier
                .values
                .push(("unrelated".into(), "untrusted".into())),
            _ => {
                let chunk = &mut carrier.values[0].1;
                chunk.replace_range(0..2, "00");
            }
        });
        assert!(snapshot::restore(&altered, 1).is_err());
    }
}
