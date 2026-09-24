mod checkpoint_agreement {
    use super::*;
    use crate::queue_checkpoint::{
        QueueCheckpointAgreement, QueueCheckpointContents, QueueCheckpointEvidence,
        QueueCheckpointProposal, QueueCheckpointReceipt,
    };

    fn contents(event_next: u64) -> QueueCheckpointContents {
        QueueCheckpointContents {
            event_next,
            message_head: 0,
            message_next: 5,
            required_message_next: 4,
            snapshot_digest: [11; 32],
            state_digest: [12; 32],
            message_digest: [13; 32],
            live_payload_digest: [14; 32],
        }
    }

    fn proposal(snapshot: &CoordinationSnapshot, event_next: u64) -> QueueCheckpointProposal {
        let resource = ResourceIdentity::new(crate::QUEUE_NAMESPACE, "q", 0, None::<String>);
        QueueCheckpointProposal::new(snapshot, &resource, [9; 16], contents(event_next), None)
            .unwrap()
    }

    fn receipt(proposal: &QueueCheckpointProposal, node: &str) -> QueueCheckpointReceipt {
        QueueCheckpointReceipt {
            proposal: proposal.digest().unwrap(),
            node: node.into(),
            instance: proposal.replicas()[node].clone(),
            contents: proposal.contents().clone(),
            capsule: [15; 32],
        }
    }

    fn complete(
        snapshot: &CoordinationSnapshot,
        proposal: QueueCheckpointProposal,
        previous: Option<&QueueCheckpointEvidence>,
    ) -> QueueCheckpointEvidence {
        let mut agreement = QueueCheckpointAgreement::new(snapshot, proposal.clone()).unwrap();
        for node in proposal.replicas().keys().rev() {
            agreement
                .record(snapshot, node, receipt(&proposal, node))
                .unwrap();
        }
        agreement.finish(snapshot, previous).unwrap()
    }

    #[test]
    fn checkpoint_counts_admitted_replicas_not_desired_placement() {
        let (snapshot, decision, _) = activated();
        assert_eq!(decision.assignment.followers, ["b", "c"]);
        let proposal = proposal(&snapshot, 10);
        assert_eq!(
            proposal.replicas().keys().cloned().collect::<Vec<_>>(),
            ["a", "b"]
        );
        for node in ["a", "b"] {
            let mut partial = QueueCheckpointAgreement::new(&snapshot, proposal.clone()).unwrap();
            partial
                .record(&snapshot, node, receipt(&proposal, node))
                .unwrap();
            assert!(partial.finish(&snapshot, None).is_err());
        }
        let evidence = complete(&snapshot, proposal, None);
        evidence.validate(&snapshot, None).unwrap();
        assert_eq!(evidence.receipts().len(), 2);
    }

    #[test]
    fn checkpoint_requires_all_three_even_with_majority_durable_policy() {
        let (mut snapshot, decision, mut quorum) = activated();
        let mut third = quorum.reports["b"].clone();
        third.node_id = "c".into();
        third.replica_process = [20; 16];
        third.storage.storage_instance = [21; 16];
        quorum.reports.insert("c".into(), third);
        snapshot.attributes.insert(
            initial_history::quorum_key(&decision.incarnation),
            serde_json::to_string(&quorum).unwrap(),
        );
        let activation = InitialHistoryActivation {
            version: 1,
            decision: decision.digest().unwrap(),
            prepared_quorum: quorum_digest(&quorum).unwrap(),
        };
        snapshot
            .attributes
            .insert(key(&decision), serde_json::to_string(&activation).unwrap());
        let proposal = proposal(&snapshot, 10);
        assert_eq!(proposal.replicas().len(), 3);
        let mut agreement = QueueCheckpointAgreement::new(&snapshot, proposal.clone()).unwrap();
        for node in ["a", "b"] {
            agreement
                .record(&snapshot, node, receipt(&proposal, node))
                .unwrap();
        }
        assert!(agreement.finish(&snapshot, None).is_err());
        complete(&snapshot, proposal, None)
            .validate(&snapshot, None)
            .unwrap();
    }

    #[test]
    fn checkpoint_exact_duplicates_are_idempotent_and_order_independent() {
        let (snapshot, _, _) = activated();
        let proposal = proposal(&snapshot, 10);
        let expected = complete(&snapshot, proposal.clone(), None);
        let mut agreement = QueueCheckpointAgreement::new(&snapshot, proposal.clone()).unwrap();
        for node in ["a", "b", "a", "b"] {
            agreement
                .record(&snapshot, node, receipt(&proposal, node))
                .unwrap();
        }
        let actual = agreement.finish(&snapshot, None).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.digest().unwrap(), expected.digest().unwrap());
        let bytes = serde_json::to_vec(&actual).unwrap();
        let read: QueueCheckpointEvidence = serde_json::from_slice(&bytes).unwrap();
        read.validate(&snapshot, None).unwrap();
        assert_eq!(read.digest().unwrap(), actual.digest().unwrap());
    }

    #[test]
    fn checkpoint_bad_identity_or_content_poison_attempt_even_after_valid_replies() {
        let (snapshot, _, _) = activated();
        let proposal = proposal(&snapshot, 10);
        for mutation in 0..12 {
            let mut bad = receipt(&proposal, "b");
            match mutation {
                0 => bad.node = "a".into(),
                1 => bad.instance.process = [90; 16],
                2 => bad.instance.storage = [91; 16],
                3 => bad.proposal = [92; 32],
                4 => bad.contents.event_next += 1,
                5 => bad.contents.message_next += 1,
                6 => bad.contents.message_head += 1,
                7 => bad.contents.required_message_next -= 1,
                8 => bad.contents.state_digest = [93; 32],
                9 => bad.contents.snapshot_digest = [94; 32],
                10 => bad.contents.live_payload_digest = [95; 32],
                _ => bad.contents.message_digest = [96; 32],
            }
            let mut agreement = QueueCheckpointAgreement::new(&snapshot, proposal.clone()).unwrap();
            for node in ["a", "b"] {
                agreement
                    .record(&snapshot, node, receipt(&proposal, node))
                    .unwrap();
            }
            assert!(
                agreement.record(&snapshot, "b", bad).is_err(),
                "mutation {mutation}"
            );
            assert!(agreement
                .record(&snapshot, "b", receipt(&proposal, "b"))
                .is_err());
            assert!(agreement.finish(&snapshot, None).is_err());
        }
        let mut agreement = QueueCheckpointAgreement::new(&snapshot, proposal.clone()).unwrap();
        assert!(agreement
            .record(&snapshot, "c", receipt(&proposal, "b"))
            .is_err());
        assert!(agreement.finish(&snapshot, None).is_err());
    }

    #[test]
    fn checkpoint_changing_local_capsule_receipt_is_a_conflict() {
        let (snapshot, _, _) = activated();
        let proposal = proposal(&snapshot, 10);
        let mut agreement = QueueCheckpointAgreement::new(&snapshot, proposal.clone()).unwrap();
        let mut report = receipt(&proposal, "a");
        agreement.record(&snapshot, "a", report.clone()).unwrap();
        report.capsule = [99; 32];
        assert!(agreement.record(&snapshot, "a", report).is_err());
        assert!(agreement.finish(&snapshot, None).is_err());
    }

    #[test]
    fn checkpoint_zero_is_an_exact_exclusive_boundary_and_offsets_are_independent() {
        let (snapshot, decision, _) = activated();
        for (event_next, message_next, required_message_next) in
            [(0, 0, 0), (1, 100, 99), (100, 1, 1)]
        {
            let mut content = contents(event_next);
            content.message_next = message_next;
            content.required_message_next = required_message_next;
            let proposal = QueueCheckpointProposal::new(
                &snapshot,
                &decision.incarnation.resource,
                [9; 16],
                content,
                None,
            )
            .unwrap();
            complete(&snapshot, proposal, None)
                .validate(&snapshot, None)
                .unwrap();
        }
        for (head, next, required) in [(6, 5, 4), (0, 5, 6)] {
            let mut content = contents(0);
            content.message_head = head;
            content.message_next = next;
            content.required_message_next = required;
            assert!(QueueCheckpointProposal::new(
                &snapshot,
                &decision.incarnation.resource,
                [9; 16],
                content,
                None
            )
            .is_err());
        }
    }

    #[test]
    fn checkpoint_replacement_advances_events_and_preserves_lineage_and_payload_bounds() {
        let (snapshot, decision, _) = activated();
        let old = complete(&snapshot, proposal(&snapshot, 10), None);
        let next = QueueCheckpointProposal::new(
            &snapshot,
            &decision.incarnation.resource,
            [10; 16],
            contents(11),
            Some(&old),
        )
        .unwrap();
        let next = complete(&snapshot, next, Some(&old));
        assert!(next.validate(&snapshot, None).is_err());
        next.validate(&snapshot, Some(&old)).unwrap();
        let other = complete(&snapshot, proposal(&snapshot, 9), None);
        assert!(next.validate(&snapshot, Some(&other)).is_err());
        assert!(old.validate(&snapshot, Some(&next)).is_err());
        for mutation in 0..4 {
            let mut content = contents(11);
            let mut candidate = [10; 16];
            match mutation {
                0 => content.event_next = 10,
                1 => content.event_next = 9,
                2 => content.message_next = 4,
                _ => candidate = [9; 16],
            }
            assert!(QueueCheckpointProposal::new(
                &snapshot,
                &decision.incarnation.resource,
                candidate,
                content,
                Some(&old)
            )
            .is_err());
        }
        let mut content = contents(10);
        content.message_head = 2;
        let with_head = QueueCheckpointProposal::new(
            &snapshot,
            &decision.incarnation.resource,
            [8; 16],
            content,
            None,
        )
        .unwrap();
        let with_head = complete(&snapshot, with_head, None);
        assert!(QueueCheckpointProposal::new(
            &snapshot,
            &decision.incarnation.resource,
            [10; 16],
            contents(11),
            Some(&with_head)
        )
        .is_err());
    }

    #[test]
    fn checkpoint_revalidates_fresh_metadata_at_completion_and_after_deserialization() {
        let (snapshot, decision, _) = activated();
        let proposal = proposal(&snapshot, 10);
        let evidence = complete(&snapshot, proposal.clone(), None);
        for mutation in 0..6 {
            let mut changed = snapshot.clone();
            let resource = &decision.incarnation.resource;
            match mutation {
                0 => {
                    changed.resources.remove(resource);
                }
                1 => {
                    changed
                        .attributes
                        .insert(promotion::pending_recovery_key(resource), "{}".into());
                }
                2 => {
                    changed.assignments.get_mut(resource).unwrap().owner = "b".into();
                }
                3 => {
                    let mut incarnation = decision.incarnation.clone();
                    incarnation.retired = true;
                    changed.attributes.insert(
                        crate::history_identity::key(resource),
                        serde_json::to_string(&incarnation).unwrap(),
                    );
                }
                4 => {
                    let mut incarnation = decision.incarnation.clone();
                    incarnation.id = [98; 16];
                    changed.attributes.insert(
                        crate::history_identity::key(resource),
                        serde_json::to_string(&incarnation).unwrap(),
                    );
                }
                _ => {
                    changed.attributes.remove(&key(&decision));
                }
            }
            let mut agreement = QueueCheckpointAgreement::new(&snapshot, proposal.clone()).unwrap();
            for node in ["a", "b"] {
                agreement
                    .record(&snapshot, node, receipt(&proposal, node))
                    .unwrap();
            }
            assert!(
                agreement.finish(&changed, None).is_err(),
                "mutation {mutation}"
            );
            assert!(evidence.validate(&changed, None).is_err());
        }
    }

    #[test]
    fn checkpoint_wire_tampering_never_becomes_valid_evidence() {
        let (snapshot, _, _) = activated();
        let evidence = complete(&snapshot, proposal(&snapshot, 10), None);
        for mutation in 0..8 {
            let mut value = serde_json::to_value(&evidence).unwrap();
            match mutation {
                0 => value["proposal"]["version"] = 2.into(),
                1 => value["proposal"]["replicas"]
                    .as_object_mut()
                    .unwrap()
                    .remove("b")
                    .map(|_| ())
                    .unwrap(),
                2 => value["receipts"]
                    .as_object_mut()
                    .unwrap()
                    .remove("b")
                    .map(|_| ())
                    .unwrap(),
                3 => value["proposal"]["activation"] = serde_json::to_value([0u8; 32]).unwrap(),
                4 => value["proposal"]["candidate"] = serde_json::to_value([0u8; 16]).unwrap(),
                5 => value["receipts"]["a"]["node"] = "b".into(),
                6 => {
                    value["proposal"]["binding"]["writer_session"] =
                        serde_json::to_value([0u8; 16]).unwrap()
                }
                _ => value["proposal"]["owner"] = "b".into(),
            }
            let bad: QueueCheckpointEvidence = serde_json::from_value(value).unwrap();
            assert!(
                bad.validate(&snapshot, None).is_err(),
                "mutation {mutation}"
            );
        }
        let mut value = serde_json::to_value(&evidence).unwrap();
        value["trusted"] = true.into();
        assert!(serde_json::from_value::<QueueCheckpointEvidence>(value).is_err());
    }

    #[test]
    fn checkpoint_learner_admission_invalidates_evidence_without_changing_activation() {
        let (mut snapshot, decision, _) = activated();
        let resource = &decision.incarnation.resource;
        let before = accepted_history(&snapshot, resource).unwrap();
        let old = complete(&snapshot, proposal(&snapshot, 10), None);
        let intent: crate::queue_learner::QueueLearner =
            serde_json::from_value(serde_json::json!({
                "version": 1, "id": ([31u8; 16].to_vec()),
                "assignment": decision.assignment, "activation": before.activation,
                "binding": before.binding, "node": "c", "process": ([32u8; 16].to_vec())
            }))
            .unwrap();
        let storage = PreparedStorageHistory {
            topic: "q".into(),
            partition: 0,
            group: None,
            stream: false,
            binding: before.binding.clone(),
            storage_instance: [33; 16],
        };
        let activation = blake3::Hash::from_bytes(before.activation);
        snapshot.attributes.insert(
            format!("fibril/learner/{activation}/\"c\""),
            serde_json::to_string(&intent).unwrap(),
        );
        snapshot.attributes.insert(
            format!(
                "fibril/learner-prepared/{}",
                blake3::Hash::from_bytes(intent.digest().unwrap())
            ),
            serde_json::to_string(&storage).unwrap(),
        );
        snapshot.attributes.insert(
            format!("fibril/learner-admissions/{activation}"),
            serde_json::to_string(&serde_json::json!({"c": {
                "intent": intent, "storage": storage,
                "message_target": 5, "event_target": 10,
                "message_next": 5, "event_next": 10
            }}))
            .unwrap(),
        );
        let after = accepted_history(&snapshot, resource).unwrap();
        assert_eq!(after.activation, before.activation);
        assert_eq!(after.replicas.len(), 3);
        assert!(old.validate(&snapshot, None).is_err());
        assert!(QueueCheckpointProposal::new(
            &snapshot,
            resource,
            [10; 16],
            contents(11),
            Some(&old)
        )
        .is_err());
        let fresh = QueueCheckpointProposal::new(&snapshot, resource, [10; 16], contents(11), None)
            .unwrap();
        complete(&snapshot, fresh, None)
            .validate(&snapshot, None)
            .unwrap();
    }
}
