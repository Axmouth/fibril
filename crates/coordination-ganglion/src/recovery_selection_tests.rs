use fibril_broker::queue_engine::StromaEvent;
use fibril_broker::recovery::{
    RecoveryReadPage, RecoveryReadSource, RecoveryRecord,
    inspection::{RecoveryPairInspector, RecoverySide},
    replay::RecoveryQueueStateArtifact,
};

fn selecting(
    version: u32,
) -> (
    CoordinationSnapshot,
    promotion::PendingRecovery,
    InitialHistoryPreparedQuorum,
) {
    let (mut snapshot, mut decision, mut quorum) = activated();
    decision.version = version;
    quorum.decision = decision.digest().unwrap();
    for receipt in quorum.reports.values_mut() {
        receipt.decision = quorum.decision;
    }
    let activation = InitialHistoryActivation {
        version: 1,
        decision: quorum.decision,
        prepared_quorum: quorum_digest(&quorum).unwrap(),
    };
    snapshot.attributes.insert(
        initial_history::key(&decision.incarnation),
        serde_json::to_string(&decision).unwrap(),
    );
    snapshot.attributes.insert(
        initial_history::quorum_key(&decision.incarnation),
        serde_json::to_string(&quorum).unwrap(),
    );
    snapshot
        .attributes
        .insert(key(&decision), serde_json::to_string(&activation).unwrap());
    let mut recovering = snapshot.clone();
    recovering.generation += 1;
    let assignment = recovering
        .assignments
        .get_mut(&decision.incarnation.resource)
        .unwrap();
    assignment.owner = "b".into();
    assignment.followers = vec!["a".into(), "c".into()];
    promotion::retain_unproven_assignments(&snapshot, &mut recovering).unwrap();
    let pending = serde_json::from_str(
        &recovering.attributes[&promotion::pending_recovery_key(&decision.incarnation.resource)],
    )
    .unwrap();
    (recovering, pending, quorum)
}

struct TestSource {
    report: BrokerSealedReplica,
    data: [Vec<RecoveryRecord>; 2],
}
fn source(
    pending: &promotion::PendingRecovery,
    quorum: &InitialHistoryPreparedQuorum,
    node: &str,
    messages: u64,
    events: Vec<StromaEvent>,
) -> TestSource {
    let data = [
        (0..messages)
            .map(|offset| RecoveryRecord {
                offset,
                flags: 0,
                headers: vec![],
                payload: vec![offset as u8],
            })
            .collect::<Vec<_>>(),
        events
            .into_iter()
            .enumerate()
            .map(|(offset, event)| RecoveryRecord {
                offset: offset as u64,
                flags: 0,
                headers: vec![],
                payload: event.encode().unwrap(),
            })
            .collect::<Vec<_>>(),
    ];
    let digest = |records: &[RecoveryRecord]| {
        let mut h = blake3::Hasher::new();
        h.update(b"fibril-retained-log-v1\0");
        h.update(&0u64.to_be_bytes());
        h.update(&(records.len() as u64).to_be_bytes());
        for r in records {
            h.update(&r.offset.to_be_bytes());
            h.update(&r.flags.to_be_bytes());
            h.update(&(r.headers.len() as u64).to_be_bytes());
            h.update(&r.headers);
            h.update(&(r.payload.len() as u64).to_be_bytes());
            h.update(&r.payload);
        }
        *h.finalize().as_bytes()
    };
    let mut history = RetainedHistoryIdentity {
        version: 2,
        storage_history: Some(quorum.reports[node].storage.clone()),
        id: [0; 32],
        message_digest: digest(&data[0]),
        event_digest: digest(&data[1]),
        snapshot_digest: None,
        message_head: 0,
        message_next: messages,
        event_head: 0,
        event_next: data[1].len() as u64,
    };
    let mut h = blake3::Hasher::new();
    h.update(b"fibril-retained-history-v1\0");
    h.update(&rmp_serde::to_vec_named(&("q", 0u32, None::<&str>, false, &history)).unwrap());
    history.id = *h.finalize().as_bytes();
    TestSource {
        report: BrokerSealedReplica {
            node_id: node.into(),
            seal: SealedReplicaFrontiers {
                request: RecoverySealRequest {
                    transition: pending.transition_digest().unwrap(),
                    fence_epoch: pending.proposed.epoch,
                },
                message_head: 0,
                message_next: messages,
                event_head: 0,
                event_next: history.event_next,
                history,
            },
        },
        data,
    }
}
fn inspect(a: &TestSource, b: &TestSource, target: Option<u64>) -> RecoveryPairInspector {
    let mut inspector = RecoveryPairInspector::new(
        "q",
        0,
        None,
        false,
        a.report.seal.clone(),
        b.report.seal.clone(),
        Default::default(),
    )
    .unwrap();
    if let Some(target) = target {
        inspector = inspector
            .with_queue_checkpoint_replay(target, Default::default(), 4096)
            .unwrap();
    }
    while let Some((side, request)) = inspector.next_read().unwrap() {
        let source = match side {
            RecoverySide::Left => a,
            RecoverySide::Right => b,
        };
        let records = &source.data[usize::from(request.source == RecoveryReadSource::Events)];
        let next = (records.len() as u64).min(request.from + request.max_records as u64);
        inspector
            .accept_page(
                side,
                RecoveryReadPage {
                    history_id: request.history_id,
                    source: request.source,
                    from: request.from,
                    next,
                    end: records.len() as u64,
                    records: records[request.from as usize..next as usize].to_vec(),
                    snapshot_bytes: vec![],
                },
            )
            .unwrap();
    }
    inspector
}
fn artifact(source: &TestSource, target: u64) -> RecoveryQueueStateArtifact {
    inspect(source, source, Some(target))
        .finish_with_queue_artifacts(4096)
        .unwrap()
        .1[0]
        .clone()
}
fn enqueue(off: u64) -> StromaEvent {
    StromaEvent::Enqueue {
        off,
        retries: 0,
        expire_at: None,
    }
}

#[test]
fn source_selection_covers_observed_tails_and_rejects_missing_comparison_or_partial_replay() {
    let (snapshot, pending, quorum) = selecting(2);
    let a = source(&pending, &quorum, "a", 1, vec![enqueue(0)]);
    let b = source(&pending, &quorum, "b", 2, vec![enqueue(0), enqueue(1)]);
    let mut witnesses = RecoveryWitnessSet::new(&snapshot, &pending).unwrap();
    let mut artifacts =
        BTreeMap::from([("a".into(), artifact(&a, 1)), ("b".into(), artifact(&b, 2))]);
    assert!(
        witnesses
            .select_queue_source(&snapshot, &artifacts, &[])
            .is_err()
    );
    for s in [&a, &b] {
        witnesses
            .record(&snapshot, &s.report.node_id, s.report.clone())
            .unwrap();
    }
    assert!(
        witnesses
            .select_queue_source(&snapshot, &artifacts, &[])
            .is_err()
    );
    let pair = inspect(&a, &b, None).finish().unwrap();
    let chosen = witnesses
        .select_queue_source(&snapshot, &artifacts, &[pair.clone()])
        .unwrap();
    assert_eq!(chosen.source_node(), "b");
    assert_eq!((chosen.event_next(), chosen.message_next()), (2, 2));
    assert_eq!(chosen.snapshot_digest(), artifacts["b"].snapshot_digest());
    assert_eq!(
        chosen.digest().unwrap(),
        witnesses
            .select_queue_source(&snapshot, &artifacts, &[pair.clone()])
            .unwrap()
            .digest()
            .unwrap()
    );
    artifacts.insert("b".into(), artifact(&b, 1));
    assert!(
        witnesses
            .select_queue_source(&snapshot, &artifacts, &[pair])
            .is_err()
    );
    let mut changed = snapshot;
    changed
        .attributes
        .remove(&promotion::pending_recovery_key(&pending.previous.resource));
    assert!(
        witnesses
            .select_queue_source(&changed, &artifacts, &[])
            .is_err()
    );
}

#[test]
fn source_selection_accepts_one_intersecting_witness_but_refuses_old_timer_origin() {
    for version in [1, 2] {
        let (snapshot, pending, quorum) = selecting(version);
        let b = source(&pending, &quorum, "b", 1, vec![enqueue(0)]);
        let artifacts = BTreeMap::from([("b".into(), artifact(&b, 1))]);
        let mut witnesses = RecoveryWitnessSet::new(&snapshot, &pending).unwrap();
        witnesses.record(&snapshot, "b", b.report.clone()).unwrap();
        assert_eq!(
            witnesses
                .select_queue_source(&snapshot, &artifacts, &[])
                .is_ok(),
            version == 2
        );
    }
}

#[test]
fn source_selection_refuses_divergence_and_crossed_incomplete_tails() {
    let (snapshot, pending, quorum) = selecting(2);
    for crossed in [false, true] {
        let a = source(
            &pending,
            &quorum,
            "a",
            if crossed { 2 } else { 1 },
            vec![enqueue(0)],
        );
        let b = source(
            &pending,
            &quorum,
            "b",
            1,
            if crossed {
                vec![enqueue(0), StromaEvent::Ack { off: 0 }]
            } else {
                vec![StromaEvent::Enqueue {
                    off: 0,
                    retries: 1,
                    expire_at: None,
                }]
            },
        );
        let artifacts = BTreeMap::from([
            ("a".into(), artifact(&a, a.report.seal.event_next)),
            ("b".into(), artifact(&b, b.report.seal.event_next)),
        ]);
        let mut witnesses = RecoveryWitnessSet::new(&snapshot, &pending).unwrap();
        for s in [&a, &b] {
            witnesses
                .record(&snapshot, &s.report.node_id, s.report.clone())
                .unwrap();
        }
        let pair = inspect(&a, &b, None).finish().unwrap();
        let error = witnesses
            .select_queue_source(&snapshot, &artifacts, &[pair])
            .unwrap_err();
        assert!(
            error.contains(if crossed {
                "both observed tails"
            } else {
                "diverge"
            }),
            "{error}"
        );
    }
}

#[test]
fn recovery_plan_binds_verified_state_and_preserves_the_pending_barrier() {
    use crate::recovery_plan::QueueRecoveryPlan;
    for count in [0, 2] {
        let (mut snapshot, pending, quorum) = selecting(2);
        let b = source(
            &pending,
            &quorum,
            "b",
            count,
            (0..count).map(enqueue).collect(),
        );
        let state_artifact = artifact(&b, count);
        let mut witnesses = RecoveryWitnessSet::new(&snapshot, &pending).unwrap();
        witnesses.record(&snapshot, "b", b.report.clone()).unwrap();
        let selected = witnesses
            .select_queue_source(
                &snapshot,
                &BTreeMap::from([("b".into(), state_artifact.clone())]),
                &[],
            )
            .unwrap();
        let plan = QueueRecoveryPlan::proposed(&snapshot, &pending, &witnesses, &selected).unwrap();
        plan.verify_artifact(&state_artifact).unwrap();
        assert!(plan.validate_committed(&snapshot).is_err());
        let key = crate::recovery_plan::key(&pending).unwrap();
        let assignments = snapshot.assignments.clone();
        let raw = serde_json::to_string(&plan).unwrap();
        snapshot.attributes.insert(key, raw.clone());
        let restored: QueueRecoveryPlan = serde_json::from_str(&raw).unwrap();
        restored.validate_committed(&snapshot).unwrap();
        assert_eq!(restored.digest().unwrap(), plan.digest().unwrap());
        assert_eq!(
            restored.binding().resource_incarnation,
            quorum.reports["b"].storage.binding.resource_incarnation
        );
        assert_ne!(
            restored.binding().writer_session,
            quorum.reports["b"].storage.binding.writer_session
        );
        assert_eq!(snapshot.assignments, assignments);
        assert!(
            snapshot
                .attributes
                .contains_key(&promotion::pending_recovery_key(&pending.previous.resource))
        );
        assert!(accepted_history(&snapshot, &pending.previous.resource).is_err());
        // A valid but different replay cannot be substituted after restart.
        let another = source(
            &pending,
            &quorum,
            "b",
            count + 1,
            (0..=count).map(enqueue).collect(),
        );
        assert!(
            restored
                .verify_artifact(&artifact(&another, count + 1))
                .is_err()
        );
        snapshot
            .attributes
            .remove(&promotion::pending_recovery_key(&pending.previous.resource));
        assert!(restored.validate_committed(&snapshot).is_err());
    }
}

#[test]
fn recovery_plan_rejects_substituted_sources_bindings_witnesses_and_transition() {
    use crate::recovery_plan::QueueRecoveryPlan;
    let (snapshot, pending, quorum) = selecting(2);
    let b = source(&pending, &quorum, "b", 1, vec![enqueue(0)]);
    let mut witnesses = RecoveryWitnessSet::new(&snapshot, &pending).unwrap();
    witnesses.record(&snapshot, "b", b.report.clone()).unwrap();
    let selected = witnesses
        .select_queue_source(
            &snapshot,
            &BTreeMap::from([("b".into(), artifact(&b, 1))]),
            &[],
        )
        .unwrap();
    let plan = QueueRecoveryPlan::proposed(&snapshot, &pending, &witnesses, &selected).unwrap();
    let original = serde_json::to_value(&plan).unwrap();
    for mutation in 0..10 {
        let mut value = original.clone();
        match mutation {
            0 => value["version"] = 2.into(),
            1 => value["selected"]["event_next"] = 0.into(),
            2 => value["selected"]["source_node"] = "a".into(),
            3 => value["selected"]["witnesses"] = serde_json::json!({}),
            4 => value["witnesses"] = serde_json::json!({}),
            5 => {
                value["binding"]["writer_session"] =
                    serde_json::json!(quorum.reports["b"].storage.binding.writer_session)
            }
            6 => value["binding"]["accepted_history"] = serde_json::to_value([0u8; 16]).unwrap(),
            7 => value["pending"]["proposed"]["epoch"] = 100.into(),
            8 => {
                value["witnesses"]["b"]["storage_history"]["storage_instance"] =
                    serde_json::to_value([9u8; 16]).unwrap()
            }
            _ => {
                value["binding"]["resource_incarnation"] = serde_json::to_value([9u8; 16]).unwrap()
            }
        }
        let bad: QueueRecoveryPlan = serde_json::from_value(value).unwrap();
        let mut stored = snapshot.clone();
        stored.attributes.insert(
            crate::recovery_plan::key(bad.pending()).unwrap(),
            serde_json::to_string(&bad).unwrap(),
        );
        assert!(
            bad.validate_committed(&stored).is_err(),
            "mutation {mutation}"
        );
    }
    // Source choice cannot outlive an added witness, even if the newly observed
    // tail would otherwise tempt a coordinator to retain its earlier proposal.
    let a = source(&pending, &quorum, "a", 2, vec![enqueue(0), enqueue(1)]);
    witnesses.record(&snapshot, "a", a.report).unwrap();
    assert!(QueueRecoveryPlan::proposed(&snapshot, &pending, &witnesses, &selected).is_err());
}
