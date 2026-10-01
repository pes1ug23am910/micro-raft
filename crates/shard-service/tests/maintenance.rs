use raft_core::membership::{
    CommittedMembership, ConfigurationEntry, ConfigurationPhase, MemberEndpoints,
};
use shard_service::controller::{Controller, ControllerCommand as C, ControllerReply, Phase};
use shard_service::machine::Machine;
use shard_service::query::{Observed, Query};
use shard_service::*;

fn apply(c: &mut Controller, command: C) -> Result<ControllerReply, Error> {
    c.apply(c.applied + 1, command).unwrap()
}
fn initial() -> Controller {
    let mut c = Controller::default();
    apply(
        &mut c,
        C::Initialize {
            groups: vec![1, 2],
            owners: vec![1],
        },
    )
    .unwrap();
    c
}
fn operation(id: u8) -> AdminOperation {
    AdminOperation::AddLearner {
        id,
        endpoints: MemberEndpoints {
            raft: format!("10.0.0.{id}:7100"),
            http: format!("10.0.0.{id}:8100"),
        },
    }
}
fn acquire(
    c: &mut Controller,
    name: &str,
    target: GroupId,
    operation: AdminOperation,
) -> MaintenanceTicket {
    let expected_generation = c.maintenance_generation();
    let ControllerReply::Maintenance(ticket) = apply(
        c,
        C::AcquireMaintenance {
            request_id: name.into(),
            target,
            operation,
            expected_generation,
        },
    )
    .unwrap() else {
        panic!()
    };
    *ticket
}
fn target_record(ticket: &MaintenanceTicket, first: u64) -> AdminRecord {
    AdminRecord {
        operation: ticket.operation.clone(),
        first_index: first,
        first_term: 2,
        joint: false,
        final_index: Some(first),
        final_term: Some(2),
    }
}
fn completion(ticket: &MaintenanceTicket, record: AdminRecord, applied: u64) -> C {
    C::CompleteMaintenance {
        request_id: ticket.request_id.clone(),
        ticket_origin: ticket.origin,
        core_request_id: ticket.core_request_id(),
        record,
        observed_applied: Origin {
            group: ticket.target,
            index: applied,
        },
    }
}
fn complete(c: &mut Controller, ticket: &MaintenanceTicket) -> MaintenanceTicket {
    let ControllerReply::Maintenance(done) =
        apply(c, completion(ticket, target_record(ticket, 100), 100)).unwrap()
    else {
        panic!()
    };
    *done
}
fn begin(name: &str) -> C {
    C::Begin {
        request_id: name.into(),
        shard: 0,
        expected_epoch: 1,
        destination: 2,
    }
}
fn reopened(c: &Controller) -> Controller {
    let c: Controller = serde_json::from_slice(&serde_json::to_vec(c).unwrap()).unwrap();
    c.validate().unwrap();
    c
}

#[test]
fn ticket_and_handoff_admission_are_mutually_exclusive_in_both_orders() {
    let mut c = initial();
    let ticket = acquire(&mut c, "membership", 2, operation(4));
    assert_eq!(apply(&mut c, begin("move")), Err(Error::Busy));
    let completed = complete(&mut c, &ticket);
    assert!(completed.completion.is_some());
    let ControllerReply::Transfer(movement) = apply(&mut c, begin("move")).unwrap() else {
        panic!()
    };
    let generation = c.maintenance_generation();
    assert_eq!(
        apply(
            &mut c,
            C::AcquireMaintenance {
                request_id: "blocked".into(),
                target: 1,
                operation: operation(5),
                expected_generation: generation
            }
        ),
        Err(Error::Busy)
    );
    let ControllerReply::Transfer(aborted) = apply(
        &mut c,
        C::Abort {
            id: movement.movement.id.clone(),
        },
    )
    .unwrap() else {
        panic!()
    };
    let Phase::Aborted { abort, .. } = aborted.phase else {
        panic!()
    };
    apply(
        &mut c,
        C::EndpointAborted {
            abort: abort.clone(),
            recovered: Origin {
                group: 1,
                index: 100,
            },
        },
    )
    .unwrap();
    assert_eq!(
        apply(
            &mut c,
            C::AcquireMaintenance {
                request_id: "blocked".into(),
                target: 1,
                operation: operation(5),
                expected_generation: generation
            }
        ),
        Err(Error::Busy)
    );
    apply(
        &mut c,
        C::EndpointAborted {
            abort,
            recovered: Origin {
                group: 2,
                index: 100,
            },
        },
    )
    .unwrap();
    let next = acquire(&mut c, "after-abort-cleanup", 1, operation(5));
    assert_eq!(next.previous_generation, Some(ticket.origin));
    c.validate().unwrap();
}

#[test]
fn stale_prevalidation_generation_cannot_acquire_after_an_intervening_membership_change() {
    let mut c = initial();
    let stale_generation = c.maintenance_generation();
    let other = acquire(&mut c, "other", 1, operation(4));
    complete(&mut c, &other);
    assert_eq!(
        apply(
            &mut c,
            C::AcquireMaintenance {
                request_id: "stale".into(),
                target: 2,
                operation: operation(5),
                expected_generation: stale_generation
            }
        ),
        Err(Error::StaleGeneration)
    );
    assert!(!c.maintenance.contains_key("stale"));
    let fresh = acquire(&mut c, "fresh", 2, operation(5));
    assert_eq!(fresh.previous_generation, Some(other.origin));
    assert_ne!(fresh.core_request_id(), other.core_request_id());
    let Observed::MaintenanceGate {
        active,
        generation,
        handoff_pending,
    } = Machine::Controller(c.clone())
        .query(&Query::MaintenanceGate)
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(active, Some(fresh.clone()));
    assert_eq!(generation, Some(fresh.origin));
    assert!(!handoff_pending);
    c.validate().unwrap();
}

#[test]
fn exact_retry_survives_reopen_and_delayed_old_completion_cannot_release_a_new_ticket() {
    let mut c = initial();
    let first = acquire(&mut c, "first", 1, operation(4));
    c = reopened(&c);
    let ControllerReply::Maintenance(retried) = apply(
        &mut c,
        C::AcquireMaintenance {
            request_id: "first".into(),
            target: 1,
            operation: operation(4),
            expected_generation: None,
        },
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(*retried, first);
    assert_eq!(
        apply(
            &mut c,
            C::AcquireMaintenance {
                request_id: "first".into(),
                target: 1,
                operation: operation(5),
                expected_generation: None
            }
        ),
        Err(Error::PayloadMismatch)
    );
    let done = complete(&mut c, &first);
    let second = acquire(&mut c, "second", 2, operation(5));
    c = reopened(&c);
    let ControllerReply::Maintenance(old) = apply(
        &mut c,
        completion(
            &first,
            done.completion.as_ref().unwrap().record.clone(),
            101,
        ),
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(*old, done);
    assert_eq!(c.active_maintenance(), Some(&second));
    assert_eq!(
        apply(&mut c, begin("must-remain-blocked")),
        Err(Error::Busy)
    );
    c.validate().unwrap();
}

#[test]
fn unknown_pending_wrong_identity_and_unapplied_membership_records_never_release() {
    let mut c = initial();
    let ticket = acquire(&mut c, "protected", 1, operation(4));
    let good = completion(&ticket, target_record(&ticket, 100), 100);
    for variant in 0..9 {
        let mut state = c.clone();
        let mut bad = good.clone();
        let C::CompleteMaintenance {
            request_id,
            ticket_origin,
            core_request_id,
            record,
            observed_applied,
        } = &mut bad
        else {
            panic!()
        };
        match variant {
            0 => *request_id = "absent".into(),
            1 => ticket_origin.index += 1,
            2 => core_request_id.push('x'),
            3 => {
                record.final_index = None;
                record.final_term = None;
            }
            4 => record.operation = operation(5),
            5 => observed_applied.group = 2,
            6 => observed_applied.index = 99,
            7 => record.first_term = 0,
            8 => record.joint = true,
            _ => unreachable!(),
        }
        assert!(apply(&mut state, bad).is_err(), "variant {variant}");
        assert_eq!(state.active_maintenance(), Some(&ticket));
        assert_eq!(apply(&mut state, begin("blocked")), Err(Error::Busy));
        state.validate().unwrap();
    }
    // Time/progress alone cannot cancel or reassign an admitted ticket.
    for _ in 0..100 {
        c.advance(c.applied + 1).unwrap();
    }
    assert_eq!(c.active_maintenance(), Some(&ticket));
    c.validate().unwrap();
}

#[test]
fn controller_reconfiguration_completion_is_in_its_own_log_order() {
    let mut c = initial();
    let ticket = acquire(
        &mut c,
        "controller-change",
        CONTROLLER,
        AdminOperation::SetVoters { voters: vec![1, 2] },
    );
    let record = AdminRecord {
        operation: ticket.operation.clone(),
        first_index: 3,
        first_term: 2,
        joint: true,
        final_index: Some(5),
        final_term: Some(2),
    };
    let mut too_early = c.clone();
    assert!(apply(&mut too_early, completion(&ticket, record.clone(), 5)).is_err());
    while c.applied < 5 {
        c.advance(c.applied + 1).unwrap();
    }
    let ControllerReply::Maintenance(done) = apply(&mut c, completion(&ticket, record, 5)).unwrap()
    else {
        panic!()
    };
    assert_eq!(done.completion.as_ref().unwrap().released.index, 6);
    assert!(c.active_maintenance().is_none());
    c.validate().unwrap();
}

#[test]
fn malformed_ticket_history_generations_payloads_and_active_overlap_fail_import() {
    let mut c = initial();
    let first = acquire(&mut c, "first", 1, operation(4));
    complete(&mut c, &first);
    let second = acquire(&mut c, "second", 2, operation(5));
    for variant in 0..6 {
        let mut bad = c.clone();
        match variant {
            0 => {
                bad.maintenance.remove("first");
            }
            1 => {
                bad.maintenance
                    .get_mut("second")
                    .unwrap()
                    .previous_generation = None
            }
            2 => {
                bad.maintenance
                    .get_mut("first")
                    .unwrap()
                    .completion
                    .as_mut()
                    .unwrap()
                    .record
                    .operation = operation(5)
            }
            3 => bad.maintenance.get_mut("second").unwrap().target = 99,
            4 => bad.maintenance.get_mut("second").unwrap().origin = first.origin,
            5 => bad.maintenance.get_mut("first").unwrap().completion = None,
            _ => unreachable!(),
        }
        assert!(bad.validate().is_err(), "variant {variant}");
    }
    let mut with_move = initial();
    let ControllerReply::Transfer(movement) = apply(&mut with_move, begin("overlap")).unwrap()
    else {
        panic!()
    };
    let mut bad = c;
    let mut movement = *movement;
    movement.movement.id.controller_index = second.origin.index + 1;
    bad.applied += 1;
    bad.routes[0].transfer = Some(movement.movement.id.clone());
    bad.transfers.insert("overlap".into(), movement);
    assert!(bad.validate().is_err());
}

#[test]
fn retained_ticket_capacity_refuses_new_work_without_evicting_exact_retries() {
    let mut c = Controller::default();
    apply(
        &mut c,
        C::Initialize {
            groups: (1..=16).collect(),
            owners: vec![1],
        },
    )
    .unwrap();
    let mut previous = None;
    // A bounded retained-history import fixture: each target adds distinct
    // learner IDs, so no ID needs to be reused to reach the global ticket cap.
    for n in 0..MAX_MAINTENANCE_TICKETS {
        let target = (n % 16 + 1) as u16;
        let index = 2 + 2 * n as u64;
        let ticket = MaintenanceTicket {
            request_id: format!("retained-{n}"),
            target,
            operation: operation((n / 16 + 4) as u8),
            origin: Origin {
                group: CONTROLLER,
                index,
            },
            previous_generation: previous,
            completion: None,
        };
        let completed = MaintenanceTicket {
            completion: Some(MaintenanceCompletion {
                record: target_record(&ticket, 100 + n as u64 / 16),
                observed_applied: Origin {
                    group: target,
                    index: 100 + n as u64 / 16,
                },
                released: Origin {
                    group: CONTROLLER,
                    index: index + 1,
                },
            }),
            ..ticket
        };
        previous = Some(completed.origin);
        c.maintenance
            .insert(completed.request_id.clone(), completed);
    }
    c.applied = 2 * MAX_MAINTENANCE_TICKETS as u64 + 1;
    c.validate().unwrap();
    let oldest = c.maintenance["retained-0"].clone();
    let generation = c.maintenance_generation();
    assert_eq!(
        apply(
            &mut c,
            C::AcquireMaintenance {
                request_id: "too-many".into(),
                target: 1,
                operation: operation(99),
                expected_generation: generation
            }
        ),
        Err(Error::Capacity)
    );
    let ControllerReply::Maintenance(retry) = apply(
        &mut c,
        C::AcquireMaintenance {
            request_id: oldest.request_id.clone(),
            target: oldest.target,
            operation: oldest.operation.clone(),
            expected_generation: None,
        },
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(*retry, oldest);
    assert_eq!(c.maintenance.len(), MAX_MAINTENANCE_TICKETS);
    assert_eq!(c.maintenance_generation(), generation);
}

#[test]
fn delayed_ticket_maps_to_the_same_retained_core_request_after_release() {
    let mut c = initial();
    let ticket = acquire(&mut c, "stable-user-id", 1, operation(4));
    let core_id = ticket.core_request_id();
    let target = CommittedMembership::bootstrap_with_group("cluster-g1".into(), vec![1, 2, 3])
        .unwrap()
        .advanced(
            10,
            2,
            &ConfigurationEntry {
                request_id: core_id.clone(),
                operation: ticket.operation.clone(),
                phase: ConfigurationPhase::Apply,
            },
        )
        .unwrap();
    let record = target.state.records[&core_id].clone();
    apply(&mut c, completion(&ticket, record.clone(), 10)).unwrap();
    apply(&mut c, begin("now-safe-to-move")).unwrap();
    assert_eq!(
        target.state.first_phase(&core_id, &ticket.operation),
        Err("request_already_recorded".into())
    );
    assert_eq!(
        target.state.first_phase(&core_id, &operation(5)),
        Err("request_payload_changed".into())
    );
    assert_eq!(target.state.records[&core_id], record);
    let Observed::Maintenance { ticket: Some(old) } = Machine::Controller(c)
        .query(&Query::Maintenance {
            request_id: "stable-user-id".into(),
        })
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(old.core_request_id(), core_id);
}

#[test]
fn same_target_completion_cannot_reuse_an_old_index_or_regress_its_term() {
    let mut c = initial();
    let first = acquire(&mut c, "first", 1, operation(4));
    apply(&mut c, completion(&first, target_record(&first, 100), 110)).unwrap();
    let second = acquire(&mut c, "second", 1, operation(5));
    for variant in 0..2 {
        let mut bad_record = target_record(&second, 111);
        if variant == 0 {
            bad_record.first_index = 110;
            bad_record.final_index = Some(110);
        } else {
            bad_record.first_term = 1;
            bad_record.final_term = Some(1);
        }
        let mut state = c.clone();
        assert_eq!(
            apply(&mut state, completion(&second, bad_record.clone(), 111)),
            Err(Error::WrongPhase)
        );
        assert_eq!(state.active_maintenance(), Some(&second));
        state.maintenance.get_mut("second").unwrap().completion = Some(MaintenanceCompletion {
            record: bad_record,
            observed_applied: Origin {
                group: 1,
                index: 111,
            },
            released: Origin {
                group: CONTROLLER,
                index: state.applied,
            },
        });
        assert!(state.validate().is_err());
    }
    apply(
        &mut c,
        completion(&second, target_record(&second, 111), 111),
    )
    .unwrap();
    c.validate().unwrap();
}
