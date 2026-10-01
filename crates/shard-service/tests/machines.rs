use shard_service::controller::{Controller, ControllerCommand as C, ControllerReply, Phase};
use shard_service::data::{DataCommand as D, DataMachine, DataReply, Slot};
use shard_service::*;

fn control(machine: &mut Controller, cmd: C) -> ControllerReply {
    machine.apply(machine.applied + 1, cmd).unwrap().unwrap()
}
fn data(machine: &mut DataMachine, cmd: D) -> DataReply {
    machine.apply(machine.applied + 1, cmd).unwrap().unwrap()
}
fn receipt(reply: DataReply) -> Receipt {
    let DataReply::Receipt(value) = reply else {
        panic!("receipt expected")
    };
    value
}
fn init() -> (Controller, DataMachine, DataMachine) {
    let mut controller = Controller::default();
    control(
        &mut controller,
        C::Initialize {
            groups: vec![1, 2],
            owners: vec![1],
        },
    );
    let mut source = DataMachine::new(1).unwrap();
    data(
        &mut source,
        D::Initialize {
            shard_count: 1,
            groups: vec![1, 2],
            owned: vec![0],
        },
    );
    let mut destination = DataMachine::new(2).unwrap();
    data(
        &mut destination,
        D::Initialize {
            shard_count: 1,
            groups: vec![1, 2],
            owned: vec![],
        },
    );
    (controller, source, destination)
}
fn begin(controller: &mut Controller, source: &mut DataMachine) -> ImageProof {
    let ControllerReply::Transfer(transfer) = control(
        controller,
        C::Begin {
            request_id: "move-1".into(),
            shard: 0,
            expected_epoch: 1,
            destination: 2,
        },
    ) else {
        panic!()
    };
    let DataReply::Fenced(image) = data(
        source,
        D::Fence {
            movement: transfer.movement,
        },
    ) else {
        panic!()
    };
    control(
        controller,
        C::Fenced {
            image: image.clone(),
        },
    );
    image
}
fn install(
    source: &DataMachine,
    destination: &mut DataMachine,
    image: &ImageProof,
) -> InstalledProof {
    data(
        destination,
        D::BeginInstall {
            image: image.clone(),
        },
    );
    let mut offset = 0;
    while offset < image.bytes {
        let (_, chunk) = source.export(&image.movement.id, offset).unwrap();
        let command = D::Chunk {
            id: image.movement.id.clone(),
            offset,
            bytes: chunk.clone(),
        };
        let first = data(destination, command.clone());
        assert_eq!(
            data(destination, command),
            first,
            "duplicate chunk must preserve progress"
        );
        offset += chunk.len() as u64;
    }
    let DataReply::Installed(installed) = data(
        destination,
        D::FinishInstall {
            id: image.movement.id.clone(),
        },
    ) else {
        panic!()
    };
    installed
}

#[test]
fn actual_values_tombstones_and_original_retry_receipts_cross_a_fenced_handoff() {
    let (mut controller, mut source, mut destination) = init();
    let session = receipt(data(
        &mut source,
        D::Register {
            shard: 0,
            epoch: 1,
            nonce: "client".into(),
        },
    ))
    .session
    .unwrap();
    data(
        &mut source,
        D::Mutate {
            shard: 0,
            epoch: 1,
            session,
            key: "deleted".into(),
            sequence: 1,
            value: Some("old".into()),
        },
    );
    let deleted = receipt(data(
        &mut source,
        D::Mutate {
            shard: 0,
            epoch: 1,
            session,
            key: "deleted".into(),
            sequence: 2,
            value: None,
        },
    ));
    let value = "v".repeat(40_000);
    let written = receipt(data(
        &mut source,
        D::Mutate {
            shard: 0,
            epoch: 1,
            session,
            key: "live".into(),
            sequence: 1,
            value: Some(value.clone()),
        },
    ));
    let image = begin(&mut controller, &mut source);
    assert!(image.bytes > CHUNK_BYTES as u64);
    assert_eq!(source.read(0, 1, "live"), Err(Error::Unavailable));
    assert_eq!(
        source
            .apply(
                source.applied + 1,
                D::Mutate {
                    shard: 0,
                    epoch: 1,
                    session,
                    key: "deleted".into(),
                    sequence: 2,
                    value: None
                }
            )
            .unwrap(),
        Err(Error::Unavailable)
    );
    let installed = install(&source, &mut destination, &image);
    assert_eq!(destination.read(0, 2, "live"), Err(Error::Unavailable));
    control(
        &mut controller,
        C::Installed {
            installation: installed,
        },
    );
    let ControllerReply::Transfer(transfer) = control(
        &mut controller,
        C::Own {
            id: image.movement.id.clone(),
        },
    ) else {
        panic!()
    };
    let Phase::Owned { ownership } = transfer.phase else {
        panic!()
    };
    // Controller finalization alone has not made the destination serve.
    assert_eq!(controller.routes[0].owner, 2);
    assert_eq!(destination.read(0, 2, "live"), Err(Error::Unavailable));
    let DataReply::Activated(activation) = data(&mut destination, D::Activate { ownership }) else {
        panic!()
    };
    control(
        &mut controller,
        C::Activated {
            activation: activation.clone(),
        },
    );
    assert_eq!(destination.read(0, 2, "live").unwrap(), Some(value.clone()));
    assert_eq!(destination.read(0, 2, "deleted").unwrap(), None);
    assert_eq!(
        receipt(data(
            &mut destination,
            D::Mutate {
                shard: 0,
                epoch: 2,
                session,
                key: "deleted".into(),
                sequence: 2,
                value: None
            }
        )),
        deleted
    );
    assert_eq!(
        receipt(data(
            &mut destination,
            D::Mutate {
                shard: 0,
                epoch: 2,
                session,
                key: "live".into(),
                sequence: 1,
                value: Some(value)
            }
        )),
        written
    );
    assert_eq!(
        destination.shards[&0].data.as_ref().unwrap().cells["deleted"],
        None
    );
    let DataReply::Cleaned { origin, .. } = data(
        &mut source,
        D::Cleanup {
            activation: activation.clone(),
        },
    ) else {
        panic!()
    };
    control(
        &mut controller,
        C::Cleaned {
            id: image.movement.id.clone(),
            cleaned: origin,
        },
    );
    control(
        &mut controller,
        C::Complete {
            id: image.movement.id,
        },
    );
    assert!(source.shards[&0].data.is_none());
    assert!(matches!(source.shards[&0].slot, Slot::Retired { .. }));
    assert_eq!(source.read(0, 1, "live"), Err(Error::StaleRoute));
    controller.validate().unwrap();
    source.validate().unwrap();
    destination.validate().unwrap();
}

#[test]
fn malformed_partial_transfer_never_serves_and_reset_retains_exact_identity() {
    let (mut controller, mut source, mut destination) = init();
    let image = begin(&mut controller, &mut source);
    data(
        &mut destination,
        D::BeginInstall {
            image: image.clone(),
        },
    );
    let (_, mut chunk) = source.export(&image.movement.id, 0).unwrap();
    chunk[0] ^= 1;
    data(
        &mut destination,
        D::Chunk {
            id: image.movement.id.clone(),
            offset: 0,
            bytes: chunk,
        },
    );
    assert_eq!(
        destination
            .apply(
                destination.applied + 1,
                D::FinishInstall {
                    id: image.movement.id.clone()
                }
            )
            .unwrap(),
        Err(Error::PayloadMismatch)
    );
    assert_eq!(destination.read(0, 2, "key"), Err(Error::Unavailable));
    let mut wrong = image.clone();
    wrong.sha256[0] ^= 1;
    assert_eq!(
        destination
            .apply(destination.applied + 1, D::ResetInstall { image: wrong })
            .unwrap(),
        Err(Error::WrongTransfer)
    );
    data(
        &mut destination,
        D::ResetInstall {
            image: image.clone(),
        },
    );
    let installed = install(&source, &mut destination, &image);
    assert_eq!(installed.image, image);
}

#[test]
fn stale_routes_payload_changes_and_session_tombstones_survive_machine_reopen() {
    let (_, mut source, _) = init();
    let registered = receipt(data(
        &mut source,
        D::Register {
            shard: 0,
            epoch: 1,
            nonce: "client".into(),
        },
    ));
    let session = registered.session.unwrap();
    let command = D::Mutate {
        shard: 0,
        epoch: 1,
        session,
        key: "key".into(),
        sequence: 1,
        value: Some("one".into()),
    };
    let first = data(&mut source, command.clone());
    let bytes = canonical_bytes(&source, MAX_ACTOR_BYTES).unwrap();
    let mut reopened: DataMachine = serde_json::from_slice(&bytes).unwrap();
    reopened.validate().unwrap();
    assert_eq!(data(&mut reopened, command), first);
    assert_eq!(
        reopened
            .apply(
                reopened.applied + 1,
                D::Mutate {
                    shard: 0,
                    epoch: 1,
                    session,
                    key: "key".into(),
                    sequence: 1,
                    value: Some("different".into())
                }
            )
            .unwrap(),
        Err(Error::PayloadMismatch)
    );
    assert_eq!(
        reopened
            .apply(
                reopened.applied + 1,
                D::Mutate {
                    shard: 0,
                    epoch: 2,
                    session,
                    key: "key".into(),
                    sequence: 2,
                    value: None
                }
            )
            .unwrap(),
        Err(Error::StaleRoute)
    );
    let closed = data(
        &mut reopened,
        D::Close {
            shard: 0,
            epoch: 1,
            session,
        },
    );
    assert_eq!(
        data(
            &mut reopened,
            D::Close {
                shard: 0,
                epoch: 1,
                session
            }
        ),
        closed
    );
    assert_eq!(
        reopened
            .apply(
                reopened.applied + 1,
                D::Mutate {
                    shard: 0,
                    epoch: 1,
                    session,
                    key: "key".into(),
                    sequence: 2,
                    value: None
                }
            )
            .unwrap(),
        Err(Error::SessionClosed)
    );
}

#[test]
fn controller_does_not_finalize_early_and_serializes_involved_groups() {
    let (mut controller, mut source, _) = init();
    let image = begin(&mut controller, &mut source);
    assert_eq!(
        controller
            .apply(
                controller.applied + 1,
                C::Own {
                    id: image.movement.id.clone()
                }
            )
            .unwrap(),
        Err(Error::WrongPhase)
    );
    assert_eq!(controller.routes[0].owner, 1);
    assert_eq!(
        controller
            .apply(
                controller.applied + 1,
                C::Begin {
                    request_id: "other".into(),
                    shard: 0,
                    expected_epoch: 1,
                    destination: 2
                }
            )
            .unwrap(),
        Err(Error::Busy)
    );
    let ControllerReply::Transfer(repeated) = control(
        &mut controller,
        C::Begin {
            request_id: "move-1".into(),
            shard: 0,
            expected_epoch: 1,
            destination: 2,
        },
    ) else {
        panic!()
    };
    assert_eq!(repeated.movement, image.movement);
    let bytes = canonical_bytes(&controller, MAX_ACTOR_BYTES).unwrap();
    let reopened: Controller = serde_json::from_slice(&bytes).unwrap();
    reopened.validate().unwrap();
    assert_eq!(reopened, controller);
}
