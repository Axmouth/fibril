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
    data: Vec<RecoveryRecord>,
}
/// Sealed queue witness whose single log holds `log` from offset zero.
fn source(
    pending: &promotion::PendingRecovery,
    quorum: &InitialHistoryPreparedQuorum,
    node: &str,
    log: Vec<StromaEvent>,
) -> TestSource {
    let data = log
        .into_iter()
        .enumerate()
        .map(|(offset, event)| RecoveryRecord {
            offset: offset as u64,
            flags: 0,
            headers: vec![],
            payload: event.encode().unwrap(),
        })
        .collect::<Vec<_>>();
    let mut h = blake3::Hasher::new();
    h.update(b"fibril-retained-log-v1\0");
    h.update(&0u64.to_be_bytes());
    h.update(&(data.len() as u64).to_be_bytes());
    for r in &data {
        h.update(&r.offset.to_be_bytes());
        h.update(&r.flags.to_be_bytes());
        h.update(&(r.headers.len() as u64).to_be_bytes());
        h.update(&r.headers);
        h.update(&(r.payload.len() as u64).to_be_bytes());
        h.update(&r.payload);
    }
    let mut history = RetainedHistoryIdentity {
        version: 2,
        storage_history: Some(quorum.reports[node].storage.clone()),
        id: [0; 32],
        snapshot_digest: None,
        logs: fibril_broker::recovery::RetainedLogs::Queue(fibril_broker::recovery::RetainedLog {
            head: 0,
            next: data.len() as u64,
            digest: *h.finalize().as_bytes(),
        }),
    };
    history.id = history.compute_id("q", 0, None, false).unwrap();
    TestSource {
        report: BrokerSealedReplica {
            node_id: node.into(),
            seal: SealedReplicaFrontiers {
                request: RecoverySealRequest {
                    transition: pending.transition_digest().unwrap(),
                    fence_epoch: pending.proposed.epoch,
                },
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
        assert_eq!(request.source, RecoveryReadSource::Log);
        let records = &source.data;
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
fn payload(body: u8) -> StromaEvent {
    StromaEvent::PayloadEnqueue {
        not_before: None,
        expire_at: None,
        headers: vec![],
        payload: vec![body],
    }
}
fn payloads(count: u64) -> Vec<StromaEvent> {
    (0..count).map(|n| payload(n as u8)).collect()
}
fn tail(source: &TestSource) -> u64 {
    source.report.seal.history.queue_log().unwrap().next
}

#[test]
fn source_selection_covers_observed_tails_and_rejects_missing_comparison_or_partial_replay() {
    let (snapshot, pending, quorum) = selecting(2);
    let a = source(&pending, &quorum, "a", payloads(1));
    let b = source(&pending, &quorum, "b", payloads(2));
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
    assert_eq!((chosen.event_next(), chosen.message_next()), (2, 0));
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
        let b = source(&pending, &quorum, "b", payloads(1));
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

/// Divergent histories block selection. So does a longest tail whose
/// witness cannot supply a complete artifact: no shorter source is promoted.
#[test]
fn source_selection_refuses_divergence_and_uncovered_longest_tail() {
    let (snapshot, pending, quorum) = selecting(2);
    for uncovered in [false, true] {
        let a = source(
            &pending,
            &quorum,
            "a",
            if uncovered {
                payloads(2)
            } else {
                vec![payload(0), StromaEvent::Ack { off: 0 }]
            },
        );
        let b = source(
            &pending,
            &quorum,
            "b",
            if uncovered {
                payloads(1)
            } else {
                vec![payload(0), StromaEvent::Nack { off: 0, requeue: true }]
            },
        );
        let mut artifacts = BTreeMap::from([("b".into(), artifact(&b, tail(&b)))]);
        if !uncovered {
            artifacts.insert("a".into(), artifact(&a, tail(&a)));
        }
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
            error.contains(if uncovered {
                "both observed tails"
            } else {
                "diverge"
            }),
            "{error}"
        );
    }
}

/// Selection takes the source tail as the stage boundary, and the source
/// artifact must match that sealed tail exactly. This is what lets the stage
/// spec carry the source log alone without a separate applied boundary.
#[test]
fn selected_boundary_equals_the_source_seal_tail() {
    let (snapshot, pending, quorum) = selecting(2);
    let b = source(&pending, &quorum, "b", payloads(3));
    let mut witnesses = RecoveryWitnessSet::new(&snapshot, &pending).unwrap();
    witnesses.record(&snapshot, "b", b.report.clone()).unwrap();
    for target in 0..3 {
        let short = BTreeMap::from([("b".into(), artifact(&b, target))]);
        assert!(witnesses.select_queue_source(&snapshot, &short, &[]).is_err());
    }
    let full = BTreeMap::from([("b".into(), artifact(&b, 3))]);
    let chosen = witnesses.select_queue_source(&snapshot, &full, &[]).unwrap();
    assert_eq!(chosen.event_next(), tail(&b));
}

#[test]
fn recovery_plan_binds_verified_state_and_preserves_the_pending_barrier() {
    use crate::recovery_plan::QueueRecoveryPlan;
    for count in [0, 2] {
        let (mut snapshot, pending, quorum) = selecting(2);
        let b = source(&pending, &quorum, "b", payloads(count));
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
        let spec = restored.stage_spec().unwrap();
        crate::recovery_candidate::replace_unavailable(&mut snapshot, |node| node == "c").unwrap();
        assert_eq!(crate::recovery_candidate::assignment(&snapshot, &pending).unwrap().owner, "c");
        restored.validate_committed(&snapshot).unwrap();
        restored.verify_artifact(&state_artifact).unwrap();
        assert_eq!(restored.stage_spec().unwrap(), spec);
        assert_eq!(witnesses.command(), &pending.seal_command().unwrap());
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
        let another = source(&pending, &quorum, "b", payloads(count + 1));
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
    let b = source(&pending, &quorum, "b", payloads(1));
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
    let a = source(&pending, &quorum, "a", payloads(2));
    witnesses.record(&snapshot, "a", a.report).unwrap();
    assert!(QueueRecoveryPlan::proposed(&snapshot, &pending, &witnesses, &selected).is_err());
}
