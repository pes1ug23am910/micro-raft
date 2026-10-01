use shard_service::controller::{
    AbortStage, Controller, ControllerCommand as C, ControllerReply, Phase, Transfer,
};
use shard_service::data::{DataCommand as D, DataMachine, DataReply, Slot};
use shard_service::machine::Machine;
use shard_service::query::{Observed, Query};
use shard_service::*;

fn control(c: &mut Controller, cmd: C) -> ControllerReply {
    c.apply(c.applied + 1, cmd).unwrap().unwrap()
}
fn data(d: &mut DataMachine, cmd: D) -> DataReply {
    d.apply(d.applied + 1, cmd).unwrap().unwrap()
}
fn transfer(reply: ControllerReply) -> Transfer {
    let ControllerReply::Transfer(t) = reply else {
        panic!("transfer")
    };
    *t
}
fn begin(c: &mut Controller, request: &str, shard: u16, epoch: u64, destination: u16) -> Transfer {
    transfer(control(
        c,
        C::Begin {
            request_id: request.into(),
            shard,
            expected_epoch: epoch,
            destination,
        },
    ))
}
fn init(owners: Vec<u16>) -> (Controller, DataMachine, DataMachine) {
    let mut c = Controller::default();
    control(
        &mut c,
        C::Initialize {
            groups: vec![1, 2],
            owners: owners.clone(),
        },
    );
    let mut source = DataMachine::new(1).unwrap();
    let mut destination = DataMachine::new(2).unwrap();
    for node in [&mut source, &mut destination] {
        data(
            node,
            D::Initialize {
                shard_count: owners.len() as u16,
                groups: vec![1, 2],
                owned: owners
                    .iter()
                    .enumerate()
                    .filter(|(_, owner)| **owner == node.group)
                    .map(|(id, _)| id as u16)
                    .collect(),
            },
        );
    }
    (c, source, destination)
}
fn register(d: &mut DataMachine, shard: u16, epoch: u64, nonce: &str) -> Origin {
    let DataReply::Receipt(r) = data(
        d,
        D::Register {
            shard,
            epoch,
            nonce: nonce.into(),
        },
    ) else {
        panic!()
    };
    r.session.unwrap()
}
fn populate(d: &mut DataMachine) -> (Origin, String) {
    let session = register(d, 0, 1, "retry-client");
    let key = (0..100)
        .map(|i| format!("key{i}"))
        .find(|key| key_shard(key, d.shard_count) == Some(0))
        .unwrap();
    data(
        d,
        D::Mutate {
            shard: 0,
            epoch: 1,
            session,
            key: key.clone(),
            sequence: 1,
            value: Some("preserved".into()),
        },
    );
    (session, key)
}
fn fence(c: &mut Controller, s: &mut DataMachine, t: &Transfer) -> ImageProof {
    let DataReply::Fenced(image) = data(
        s,
        D::Fence {
            movement: t.movement.clone(),
        },
    ) else {
        panic!()
    };
    control(
        c,
        C::Fenced {
            image: image.clone(),
        },
    );
    image
}
fn install(s: &DataMachine, d: &mut DataMachine, image: &ImageProof) -> InstalledProof {
    data(
        d,
        D::BeginInstall {
            image: image.clone(),
        },
    );
    let mut offset = 0;
    while offset < image.bytes {
        let (_, bytes) = s.export(&image.movement.id, offset).unwrap();
        let count = bytes.len() as u64;
        data(
            d,
            D::Chunk {
                id: image.movement.id.clone(),
                offset,
                bytes,
            },
        );
        offset += count;
    }
    let DataReply::Installed(proof) = data(
        d,
        D::FinishInstall {
            id: image.movement.id.clone(),
        },
    ) else {
        panic!()
    };
    proof
}
fn abort(c: &mut Controller, t: &Transfer) -> AbortProof {
    let t = transfer(control(
        c,
        C::Abort {
            id: t.movement.id.clone(),
        },
    ));
    let Phase::Aborted { abort, .. } = t.phase else {
        panic!()
    };
    abort
}
fn recover(d: &mut DataMachine, abort: &AbortProof) -> AbortRecord {
    let DataReply::Aborted(record) = data(
        d,
        D::AbortTransfer {
            abort: abort.clone(),
        },
    ) else {
        panic!()
    };
    record
}
fn report(c: &mut Controller, record: &AbortRecord) -> Transfer {
    transfer(control(
        c,
        C::EndpointAborted {
            abort: record.abort.clone(),
            recovered: record.recovered,
        },
    ))
}
fn complete(
    c: &mut Controller,
    s: &mut DataMachine,
    d: &mut DataMachine,
    t: &Transfer,
) -> ActivationProof {
    let image = fence(c, s, t);
    let installation = install(s, d, &image);
    control(c, C::Installed { installation });
    let t = transfer(control(
        c,
        C::Own {
            id: t.movement.id.clone(),
        },
    ));
    let Phase::Owned { ownership } = t.phase else {
        panic!()
    };
    let DataReply::Activated(activation) = data(d, D::Activate { ownership }) else {
        panic!()
    };
    control(
        c,
        C::Activated {
            activation: activation.clone(),
        },
    );
    let DataReply::Cleaned { origin, .. } = data(
        s,
        D::Cleanup {
            activation: activation.clone(),
        },
    ) else {
        panic!()
    };
    control(
        c,
        C::Cleaned {
            id: image.movement.id.clone(),
            cleaned: origin,
        },
    );
    control(
        c,
        C::Complete {
            id: image.movement.id,
        },
    );
    activation
}
fn reopened<T: serde::Serialize + serde::de::DeserializeOwned>(value: &T) -> T {
    serde_json::from_slice(&serde_json::to_vec(value).unwrap()).unwrap()
}

#[test]
fn abort_before_fence_retains_route_until_both_endpoints_and_rejects_delayed_work() {
    let (mut c, mut s, mut d) = init(vec![1]);
    let (_, key) = populate(&mut s);
    let t = begin(&mut c, "first", 0, 1, 2);
    // A stale coordinator has prepared its Fence request but it has not yet
    // entered the source's log when the abort cleanup commits.
    let mut hypothetical = s.clone();
    let DataReply::Fenced(old_image) = data(
        &mut hypothetical,
        D::Fence {
            movement: t.movement.clone(),
        },
    ) else {
        panic!()
    };
    let proof = abort(&mut c, &t);
    let first = recover(&mut s, &proof);
    assert!(!report(&mut c, &first).complete());
    assert_eq!(c.routes[0].owner, 1);
    assert_eq!(c.routes[0].epoch, 1);
    assert_eq!(
        c.apply(
            c.applied + 1,
            C::Begin {
                request_id: "blocked".into(),
                shard: 0,
                expected_epoch: 1,
                destination: 2
            }
        )
        .unwrap(),
        Err(Error::Busy)
    );
    assert_eq!(s.read(0, 1, &key).unwrap().as_deref(), Some("preserved"));
    assert_eq!(
        s.apply(
            s.applied + 1,
            D::Fence {
                movement: t.movement.clone()
            }
        )
        .unwrap(),
        Err(Error::Aborted)
    );
    let second = recover(&mut d, &proof);
    assert!(report(&mut c, &second).complete());
    assert_eq!(c.routes[0].transfer, None);
    assert_eq!(
        d.apply(d.applied + 1, D::BeginInstall { image: old_image })
            .unwrap(),
        Err(Error::Aborted)
    );
    let next = begin(&mut c, "next", 0, 1, 2);
    assert_eq!(next.movement.id.epoch, t.movement.id.epoch);
    assert_ne!(next.movement.id, t.movement.id);
    // A duplicated old cleanup result cannot clear the new active transfer.
    assert_eq!(recover(&mut s, &proof), first);
    assert!(report(&mut c, &first).complete());
    assert_eq!(c.routes[0].transfer, Some(next.movement.id));
    c.validate().unwrap();
    s.validate().unwrap();
    d.validate().unwrap();
}

#[test]
fn abort_partial_or_finished_install_restores_data_and_survives_lost_cleanup_replies() {
    for finished in [false, true] {
        let (mut c, mut s, mut d) = init(vec![1]);
        let (_, key) = populate(&mut s);
        let t = begin(&mut c, "move", 0, 1, 2);
        let image = fence(&mut c, &mut s, &t);
        let installation = if finished {
            Some(install(&s, &mut d, &image))
        } else {
            data(
                &mut d,
                D::BeginInstall {
                    image: image.clone(),
                },
            );
            let (_, bytes) = s.export(&image.movement.id, 0).unwrap();
            data(
                &mut d,
                D::Chunk {
                    id: image.movement.id.clone(),
                    offset: 0,
                    bytes: bytes[..bytes.len() / 2].to_vec(),
                },
            );
            None
        };
        if let Some(installation) = &installation {
            control(
                &mut c,
                C::Installed {
                    installation: installation.clone(),
                },
            );
        }
        let proof = abort(&mut c, &t);
        let a = recover(&mut s, &proof);
        let b = recover(&mut d, &proof);
        // Lost endpoint responses and coordinator restart reconstruct the same
        // first cleanup receipts from retained tombstones.
        c = reopened(&c);
        s = reopened(&s);
        d = reopened(&d);
        c.validate().unwrap();
        s.validate().unwrap();
        d.validate().unwrap();
        assert_eq!(recover(&mut s, &proof), a);
        assert_eq!(recover(&mut d, &proof), b);
        assert!(
            matches!(Machine::Data(d.clone()).query(&Query::Aborted{id:t.movement.id.clone()}).unwrap(),Observed::Aborted{record:Some(record),..} if record==b)
        );
        report(&mut c, &a);
        assert!(report(&mut c, &b).complete());
        assert_eq!(s.read(0, 1, &key).unwrap().as_deref(), Some("preserved"));
        assert!(!d.shards.contains_key(&0));
        assert_eq!(
            c.apply(
                c.applied + 1,
                C::Own {
                    id: t.movement.id.clone()
                }
            )
            .unwrap(),
            Err(Error::WrongPhase)
        );
        assert_eq!(
            d.apply(
                d.applied + 1,
                D::Chunk {
                    id: t.movement.id.clone(),
                    offset: 0,
                    bytes: vec![1]
                }
            )
            .unwrap(),
            Err(Error::Aborted)
        );
        if let Some(installation) = installation {
            let ownership = OwnershipProof {
                installation,
                controller: Origin {
                    group: 0,
                    index: c.applied + 1,
                },
            };
            assert_eq!(
                d.apply(d.applied + 1, D::Activate { ownership }).unwrap(),
                Err(Error::Aborted)
            );
        }
    }
}

#[test]
fn ownership_wins_before_abort_so_destination_only_rolls_forward() {
    let (mut c, mut s, mut d) = init(vec![1]);
    populate(&mut s);
    let t = begin(&mut c, "move", 0, 1, 2);
    let image = fence(&mut c, &mut s, &t);
    let installation = install(&s, &mut d, &image);
    control(&mut c, C::Installed { installation });
    let owned = transfer(control(
        &mut c,
        C::Own {
            id: t.movement.id.clone(),
        },
    ));
    assert_eq!(
        c.apply(
            c.applied + 1,
            C::Abort {
                id: t.movement.id.clone()
            }
        )
        .unwrap(),
        Err(Error::WrongPhase)
    );
    let Phase::Owned { ownership } = owned.phase else {
        panic!()
    };
    let first = data(
        &mut d,
        D::Activate {
            ownership: ownership.clone(),
        },
    );
    assert_eq!(data(&mut d, D::Activate { ownership }), first);
    assert_eq!(c.routes[0].owner, 2);
    assert!(matches!(d.shards[&0].slot, Slot::Active { epoch: 2, .. }));
}

#[test]
fn abort_return_move_restores_source_activation_and_destination_retired_epoch_floor() {
    let (mut c, mut one, mut two) = init(vec![1]);
    let (session, key) = populate(&mut one);
    let first = begin(&mut c, "outward", 0, 1, 2);
    let activated = complete(&mut c, &mut one, &mut two, &first);
    let old_retired = one.shards[&0].clone();
    assert!(matches!(old_retired.slot, Slot::Retired { .. }));
    let returning = begin(&mut c, "return-aborted", 0, 2, 1);
    let image = fence(&mut c, &mut two, &returning);
    let installed = install(&two, &mut one, &image);
    control(
        &mut c,
        C::Installed {
            installation: installed,
        },
    );
    let proof = abort(&mut c, &returning);
    let source = recover(&mut two, &proof);
    let destination = recover(&mut one, &proof);
    report(&mut c, &source);
    report(&mut c, &destination);
    assert_eq!(one.shards[&0], old_retired);
    assert!(
        matches!(&two.shards[&0].slot,Slot::Active{epoch:2,activation:Some(proof)} if *proof==activated)
    );
    assert_eq!(two.read(0, 2, &key).unwrap().as_deref(), Some("preserved"));
    let retried = data(
        &mut two,
        D::Mutate {
            shard: 0,
            epoch: 2,
            session,
            key: key.clone(),
            sequence: 1,
            value: Some("preserved".into()),
        },
    );
    assert!(
        matches!(retried,DataReply::Receipt(receipt) if receipt.origin.group==1 && receipt.epoch==1)
    );
    // A new exact identity at the same proposed epoch may complete, while all
    // messages for the abandoned return remain rejected after that completion.
    let next = begin(&mut c, "return-success", 0, 2, 1);
    assert_ne!(next.movement.id, returning.movement.id);
    complete(&mut c, &mut two, &mut one, &next);
    assert_eq!(one.read(0, 3, &key).unwrap().as_deref(), Some("preserved"));
    assert_eq!(
        two.apply(
            two.applied + 1,
            D::Fence {
                movement: returning.movement
            }
        )
        .unwrap(),
        Err(Error::Aborted)
    );
    assert_eq!(
        one.apply(one.applied + 1, D::BeginInstall { image })
            .unwrap(),
        Err(Error::Aborted)
    );
    c.validate().unwrap();
    one.validate().unwrap();
    two.validate().unwrap();
}

#[test]
fn full_destination_can_abort_rejected_install_without_evicting_retention_or_source_data() {
    let (mut c, mut s, mut d) = init(vec![1, 2]);
    let (_, key) = populate(&mut s);
    for number in 0..MAX_SESSIONS {
        register(&mut d, 1, 1, &format!("retained-{number}"));
    }
    let prior = d.shards[&1].clone();
    let t = begin(&mut c, "capacity", 0, 1, 2);
    let image = fence(&mut c, &mut s, &t);
    assert_eq!(
        d.apply(d.applied + 1, D::BeginInstall { image }).unwrap(),
        Err(Error::Capacity)
    );
    assert_eq!(s.read(0, 1, &key), Err(Error::Unavailable));
    let proof = abort(&mut c, &t);
    let a = recover(&mut s, &proof);
    let b = recover(&mut d, &proof);
    report(&mut c, &a);
    assert!(report(&mut c, &b).complete());
    assert_eq!(d.shards[&1], prior);
    assert_eq!(s.read(0, 1, &key).unwrap().as_deref(), Some("preserved"));
    assert_eq!(d.aborted.len(), 1);
    assert_eq!(
        d.shards[&1].data.as_ref().unwrap().sessions.len(),
        MAX_SESSIONS
    );
    d.validate().unwrap();
}

#[test]
fn oversized_source_image_refusal_can_release_controller_started_without_fencing() {
    let (mut c, mut s, mut d) = init(vec![1]);
    let session = register(&mut s, 0, 1, "large");
    for number in 0..24 {
        data(
            &mut s,
            D::Mutate {
                shard: 0,
                epoch: 1,
                session,
                key: format!("key{number}"),
                sequence: 1,
                value: Some("x".repeat(MAX_VALUE_BYTES)),
            },
        );
    }
    let t = begin(&mut c, "oversized", 0, 1, 2);
    assert_eq!(
        s.apply(
            s.applied + 1,
            D::Fence {
                movement: t.movement.clone()
            }
        )
        .unwrap(),
        Err(Error::Capacity)
    );
    assert!(matches!(s.shards[&0].slot, Slot::Active { epoch: 1, .. }));
    let proof = abort(&mut c, &t);
    let a = recover(&mut s, &proof);
    let b = recover(&mut d, &proof);
    report(&mut c, &a);
    assert!(report(&mut c, &b).complete());
    assert_eq!(c.routes[0].transfer, None);
    assert!(s.read(0, 1, "key23").unwrap().is_some());
}

#[test]
fn impossible_abort_receipts_and_changed_proofs_do_not_clear_an_incomplete_decision() {
    let (mut c, mut s, mut d) = init(vec![1]);
    populate(&mut s);
    let t = begin(&mut c, "abort", 0, 1, 2);
    let image = fence(&mut c, &mut s, &t);
    let installed = install(&s, &mut d, &image);
    control(
        &mut c,
        C::Installed {
            installation: installed.clone(),
        },
    );
    let proof = abort(&mut c, &t);
    for origin in [
        image.fence,
        installed.installed,
        Origin {
            group: 99,
            index: 99,
        },
    ] {
        assert!(c
            .apply(
                c.applied + 1,
                C::EndpointAborted {
                    abort: proof.clone(),
                    recovered: origin
                }
            )
            .unwrap()
            .is_err());
    }
    let first = recover(&mut s, &proof);
    let mut changed = proof.clone();
    changed.controller.index += 1;
    assert_eq!(
        s.apply(s.applied + 1, D::AbortTransfer { abort: changed })
            .unwrap(),
        Err(Error::PayloadMismatch)
    );
    report(&mut c, &first);
    let mut wrong = first.clone();
    wrong.recovered.index += 1;
    assert_eq!(
        c.apply(
            c.applied + 1,
            C::EndpointAborted {
                abort: proof,
                recovered: wrong.recovered
            }
        )
        .unwrap(),
        Err(Error::PayloadMismatch)
    );
    assert!(matches!(
        &c.transfers["abort"].phase,
        Phase::Aborted {
            before: AbortStage::Installed { .. },
            destination_recovered: None,
            ..
        }
    ));
}

#[test]
fn frozen_image_and_prior_provenance_corruption_are_rejected_on_snapshot_validation() {
    let (mut c, mut s, mut d) = init(vec![1]);
    let (_, key) = populate(&mut s);
    let t = begin(&mut c, "frozen", 0, 1, 2);
    let image = fence(&mut c, &mut s, &t);
    install(&s, &mut d, &image);
    for mut bad in [s.clone(), d.clone()] {
        bad.shards
            .get_mut(&0)
            .unwrap()
            .data
            .as_mut()
            .unwrap()
            .cells
            .insert(key.clone(), Some("changed".into()));
        assert!(bad.validate().is_err());
    }
    let mut bad = s.clone();
    if let Slot::Fenced { image, .. } = &mut bad.shards.get_mut(&0).unwrap().slot {
        image.retention.sessions += 1;
    }
    assert!(bad.validate().is_err());
}

#[test]
fn imported_duplicate_transfer_identity_and_receipts_beyond_a_source_fence_reject() {
    let (mut c, mut s, mut d) = init(vec![1]);
    populate(&mut s);
    let t = begin(&mut c, "original", 0, 1, 2);
    let proof = abort(&mut c, &t);
    let a = recover(&mut s, &proof);
    let b = recover(&mut d, &proof);
    report(&mut c, &a);
    report(&mut c, &b);
    let mut duplicate = c.transfers["original"].clone();
    duplicate.request_id = "different-request".into();
    c.transfers.insert(duplicate.request_id.clone(), duplicate);
    assert!(c.validate().is_err());
    let (mut c, mut s, _) = init(vec![1]);
    populate(&mut s);
    let t = begin(&mut c, "fenced", 0, 1, 2);
    let image = fence(&mut c, &mut s, &t);
    let shard = s.shards.get_mut(&0).unwrap();
    let record = shard
        .data
        .as_mut()
        .unwrap()
        .sessions
        .values_mut()
        .next()
        .unwrap()
        .keys
        .values_mut()
        .next()
        .unwrap();
    record.receipt.origin.index = image.fence.index + 1;
    s.applied = image.fence.index + 2;
    let body = shard_service::data::ShardImage {
        version: 1,
        proof_identity: image.movement.clone(),
        fence: image.fence,
        shard_count: 1,
        data: shard.data.clone().unwrap(),
    };
    let bytes = canonical_bytes(&body, MAX_TRANSFER_BYTES).unwrap();
    if let Slot::Fenced { image, .. } = &mut shard.slot {
        image.bytes = bytes.len() as u64;
        image.sha256 = sha256(&bytes);
    }
    assert!(
        s.validate().is_err(),
        "a matching digest does not make a future receipt part of an earlier source fence"
    );
}

#[test]
fn restored_routes_must_match_complete_and_aborted_ownership_history() {
    let (mut c, mut s, mut d) = init(vec![1]);
    let t = begin(&mut c, "outbound", 0, 1, 2);
    complete(&mut c, &mut s, &mut d, &t);
    c.validate().unwrap();
    let mut bad = c.clone();
    bad.routes[0].epoch = 99;
    assert!(bad.validate().unwrap_err().contains("ownership history"));
    let mut bad = c.clone();
    bad.routes[0].owner = 1;
    assert!(bad.validate().unwrap_err().contains("ownership history"));
    let mut bad = c.clone();
    let Phase::Complete {
        activation,
        completed,
        ..
    } = &mut bad.transfers.get_mut("outbound").unwrap().phase
    else {
        panic!()
    };
    completed.index = activation.ownership.controller.index;
    assert!(bad.validate().unwrap_err().contains("completion proof"));

    let back = begin(&mut c, "return", 0, 2, 1);
    let proof = abort(&mut c, &back);
    report(&mut c, &recover(&mut d, &proof));
    report(&mut c, &recover(&mut s, &proof));
    c.validate().unwrap();
    let mut bad = c.clone();
    bad.routes[0].epoch = 3;
    bad.routes[0].owner = 1;
    assert!(bad.validate().unwrap_err().contains("ownership history"));
    let mut empty = Controller::default();
    control(
        &mut empty,
        C::Initialize {
            groups: vec![1, 2],
            owners: vec![1],
        },
    );
    empty.routes[0].epoch = 2;
    assert!(empty.validate().unwrap_err().contains("ownership history"));
}

fn fill_other_shard_to_capacity(d: &mut DataMachine, shard: u16) {
    let session = register(d, shard, 1, "capacity-filler");
    let mut next_key = 0;
    let mut fresh_key = || loop {
        let key = format!("capacity-{next_key}");
        next_key += 1;
        if key_shard(&key, 3) == Some(shard) {
            break key;
        }
    };
    let operation = |key: String, length| D::Mutate {
        shard,
        epoch: 1,
        session,
        key,
        sequence: 1,
        value: Some("x".repeat(length)),
    };
    let blocked_key = loop {
        let key = fresh_key();
        match d
            .apply(d.applied + 1, operation(key.clone(), MAX_VALUE_BYTES))
            .unwrap()
        {
            Ok(DataReply::Receipt(_)) => {}
            Err(Error::Capacity) => break key,
            other => panic!("unexpected capacity fill result {other:?}"),
        }
    };
    // Find the largest admitted final value; otherwise an arbitrary residual
    // gap could accidentally hide the missing phase metadata reservation.
    let (mut low, mut high) = (0, MAX_VALUE_BYTES);
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        let mut probe = d.clone();
        if probe
            .apply(probe.applied + 1, operation(blocked_key.clone(), mid))
            .unwrap()
            .is_ok()
        {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    data(d, operation(blocked_key, low));
    assert_eq!(
        d.apply(d.applied + 1, operation(fresh_key(), 0)).unwrap(),
        Err(Error::Capacity)
    );
    d.validate().unwrap();
}

#[test]
fn unrelated_writes_cannot_consume_post_ownership_activation_or_cleanup_headroom() {
    let (mut c, mut source, mut destination) = init(vec![1, 1, 2]);
    // An empty moved shard makes the cleanup's new proof larger than the data
    // it removes, so both destination activation and source cleanup need room.
    let t = begin(&mut c, "near-capacity", 0, 1, 2);
    let image = fence(&mut c, &mut source, &t);
    let installation = install(&source, &mut destination, &image);
    control(&mut c, C::Installed { installation });
    fill_other_shard_to_capacity(&mut source, 1);
    fill_other_shard_to_capacity(&mut destination, 2);
    // Advance-only/no-op progress cannot make an admitted full image too large
    // to validate. Jump only this independent pure-state fixture's watermark;
    // normal application still requires each consecutive committed index.
    for mut future in [source.clone(), destination.clone()] {
        future.applied = u64::MAX;
        future.validate().unwrap();
    }
    let t = transfer(control(
        &mut c,
        C::Own {
            id: t.movement.id.clone(),
        },
    ));
    let Phase::Owned { ownership } = t.phase else {
        panic!()
    };
    let activation_reply = destination
        .apply(destination.applied + 1, D::Activate { ownership })
        .unwrap()
        .expect("activation must fit after controller Own");
    let DataReply::Activated(activation) = activation_reply else {
        panic!()
    };
    control(
        &mut c,
        C::Activated {
            activation: activation.clone(),
        },
    );
    let cleanup_reply = source
        .apply(source.applied + 1, D::Cleanup { activation })
        .unwrap()
        .expect("cleanup must fit after checked activation");
    let DataReply::Cleaned { origin, .. } = cleanup_reply else {
        panic!()
    };
    control(
        &mut c,
        C::Cleaned {
            id: image.movement.id.clone(),
            cleaned: origin,
        },
    );
    control(
        &mut c,
        C::Complete {
            id: image.movement.id,
        },
    );
    source.validate().unwrap();
    destination.validate().unwrap();
    c.validate().unwrap();
}
