use thetube::storage::*;

fn make_test_store() -> Box<dyn Storage> {
    // make testdata dir
    std::fs::create_dir_all("test_data").unwrap();
    // make random temp filename to avoid conflicts
    let filename = format!("test_data/{}", fastrand::u64(..));
    Box::new(make_rocksdb_store(&filename).unwrap())
}

#[tokio::test]
async fn append_and_fetch() {
    let store = make_test_store(); // returns dyn Storage boxed

    let topic = "t1".to_string();
    let group = "g1".to_string();

    let o1 = store.append(&topic, 0, b"hello").await.unwrap();
    let o2 = store.append(&topic, 0, b"world").await.unwrap();

    assert_eq!(o1, 0);
    assert_eq!(o2, 1);

    let msgs = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].message.payload, b"hello");
    assert_eq!(msgs[1].message.payload, b"world");
}

#[tokio::test]
async fn inflight_and_ack() {
    let store = make_test_store();

    let topic = "t".to_string();
    let group = "g".to_string();

    store.append(&topic, 0, b"a").await.unwrap();
    store.append(&topic, 0, b"b").await.unwrap();

    let msgs = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 2);

    store
        .mark_inflight(&topic, 0, &group, msgs[0].delivery_tag, 999999)
        .await
        .unwrap();
    store
        .ack(&topic, 0, &group, msgs[0].delivery_tag)
        .await
        .unwrap();

    let msgs2 = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();
    assert_eq!(msgs2.len(), 1);
}

#[tokio::test]
async fn redelivery() {
    let store = make_test_store();
    let topic = "t".to_string();
    let group = "g".to_string();

    store.append(&topic, 0, b"x").await.unwrap();

    let msgs = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();
    let off = msgs[0].delivery_tag;

    store
        .mark_inflight(&topic, 0, &group, off, 0)
        .await
        .unwrap(); // already expired

    let expired = store.list_expired(100).await.unwrap();
    assert_eq!(expired.len(), 1);
}

#[tokio::test]
async fn out_of_order_acks() {
    let store = make_test_store();

    let topic = "t".to_string();
    let group = "g".to_string();

    for i in 0..5 {
        store
            .append(&topic, 0, format!("msg-{i}").as_bytes())
            .await
            .unwrap();
    }

    let msgs = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 5);

    // ACK messages 2 and 4 first
    store
        .mark_inflight(&topic, 0, &group, 2, 1_000_000)
        .await
        .unwrap();
    store.ack(&topic, 0, &group, 2).await.unwrap();

    store
        .mark_inflight(&topic, 0, &group, 4, 1_000_000)
        .await
        .unwrap();
    store.ack(&topic, 0, &group, 4).await.unwrap();

    let msgs2 = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();

    // Offsets 0,1,3 are remaining
    let offsets: Vec<_> = msgs2.iter().map(|m| m.message.offset).collect();
    assert_eq!(offsets, vec![0, 1, 3]);
}

#[tokio::test]
async fn redelivery_after_expiration() {
    let store = make_test_store();
    let topic = "redel".to_string();
    let group = "g".to_string();

    let off = store.append(&topic, 0, b"a").await.unwrap();

    let msgs = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 1);

    // Mark inflight with a short deadline
    store
        .mark_inflight(&topic, 0, &group, off, 10)
        .await
        .unwrap();

    // Expired now
    let expired = store.list_expired(20).await.unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].delivery_tag, off);

    // After expiration, fetch_available should NOT see inflight entry
    // (because redelivery clears it — your code will need to)
}

#[tokio::test]
async fn multiple_groups_independent_offsets() {
    let store = make_test_store();
    let topic = "multi".to_string();
    let g1 = "g1".to_string();
    let g2 = "g2".to_string();

    for i in 0..3 {
        store
            .append(&topic, 0, format!("msg-{i}").as_bytes())
            .await
            .unwrap();
    }

    // Group 1 fetches and acks msg 0
    let msgs = store.fetch_available(&topic, 0, &g1, 0, 10).await.unwrap();
    assert_eq!(msgs.len(), 3);
    store.mark_inflight(&topic, 0, &g1, 0, 999).await.unwrap();
    store.ack(&topic, 0, &g1, 0).await.unwrap();

    // Group 2 should still see all messages
    let msgs2 = store.fetch_available(&topic, 0, &g2, 0, 10).await.unwrap();
    assert_eq!(msgs2.len(), 3);
}

#[tokio::test]
async fn fetch_max_limit_respected() {
    let store = make_test_store();
    let topic = "limit".to_string();
    let group = "g".to_string();

    for i in 0..10 {
        store
            .append(&topic, 0, format!("msg-{i}").as_bytes())
            .await
            .unwrap();
    }

    let msgs = store
        .fetch_available(&topic, 0, &group, 0, 3)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 3);
}

#[tokio::test]
async fn out_of_order_ack_behavior() {
    let store = make_test_store();
    let topic = "topic1".to_string();
    let group = "group1".to_string();

    // Write 3 messages: 0,1,2
    for i in 0..3 {
        store
            .append(&topic, 0, format!("m{i}").as_bytes())
            .await
            .unwrap();
    }

    let msgs = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 3);

    // ACK 2, skip 1
    store
        .mark_inflight(&topic, 0, &group, 2, 999)
        .await
        .unwrap();
    store.ack(&topic, 0, &group, 2).await.unwrap();

    // ACK 0
    store
        .mark_inflight(&topic, 0, &group, 0, 999)
        .await
        .unwrap();
    store.ack(&topic, 0, &group, 0).await.unwrap();

    let msgs2 = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();
    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs2[0].message.offset, 1);
}

#[tokio::test]
async fn inflight_messages_not_fetched() {
    let store = make_test_store();
    let topic = "t2".to_string();
    let group = "g".to_string();

    store.append(&topic, 0, b"a").await.unwrap(); // offset 0
    store.append(&topic, 0, b"b").await.unwrap(); // offset 1

    store
        .mark_inflight(&topic, 0, &group, 0, 1_000_000)
        .await
        .unwrap();

    let msgs = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].message.offset, 1);
}

#[tokio::test]
async fn expired_messages_are_redelivered() {
    let store = make_test_store();
    let topic = "t3".to_string();
    let group = "g".to_string();

    let off = store.append(&topic, 0, b"x").await.unwrap();

    // Fetch and mark inflight
    let msgs = store
        .fetch_available(&topic, 0, &group, 0, 1)
        .await
        .unwrap();
    assert_eq!(msgs[0].message.offset, off);

    store
        .mark_inflight(&topic, 0, &group, off, 10)
        .await
        .unwrap(); // expired at ts>10

    // Should be expired
    let expired = store.list_expired(11).await.unwrap();
    assert_eq!(expired.len(), 1);

    // After expiration, message shouldn't be "inflight" anymore
    // (you will add this logic inside broker layer or storage)
}

#[tokio::test]
async fn consumer_groups_are_isolated() {
    let store = make_test_store();
    let t = "topicX".to_string();
    let g1 = "G1".to_string();
    let g2 = "G2".to_string();

    for i in 0..3 {
        store
            .append(&t, 0, format!("v{i}").as_bytes())
            .await
            .unwrap();
    }

    // Group 1 acks message 0
    store.mark_inflight(&t, 0, &g1, 0, 999).await.unwrap();
    store.ack(&t, 0, &g1, 0).await.unwrap();

    // Group 2 should still see all 3
    let msgs_g2 = store.fetch_available(&t, 0, &g2, 0, 10).await.unwrap();
    assert_eq!(msgs_g2.len(), 3);
}

#[tokio::test]
async fn prefix_isolation() {
    let store = make_test_store();

    let t1 = "A".to_string();
    let t2 = "B".to_string();
    let g = "G".to_string();

    store.append(&t1, 0, b"x").await.unwrap();
    store.append(&t2, 0, b"y").await.unwrap();

    let msgs = store.fetch_available(&t1, 0, &g, 0, 10).await.unwrap();

    // Must not return messages for topic B
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].message.payload, b"x");
}

#[tokio::test]
async fn large_offset_ranges() {
    let store = make_test_store();
    let topic = "big".to_string();
    let group = "g".to_string();

    for i in 0..2000 {
        store
            .append(&topic, 0, format!("{i}").as_bytes())
            .await
            .unwrap();
    }

    let msgs = store
        .fetch_available(&topic, 0, &group, 1500, 100)
        .await
        .unwrap();

    assert_eq!(msgs[0].message.offset, 1500);
    assert_eq!(msgs.len(), 100);
}

#[tokio::test]
async fn no_double_delivery_after_ack() {
    let store = make_test_store();
    let topic = "nodup".to_string();
    let group = "g".to_string();

    store.append(&topic, 0, b"a").await.unwrap();
    store.append(&topic, 0, b"b").await.unwrap();

    let _msgs = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();

    // ACK the first
    store
        .mark_inflight(&topic, 0, &group, 0, 999)
        .await
        .unwrap();
    store.ack(&topic, 0, &group, 0).await.unwrap();

    let msgs2 = store
        .fetch_available(&topic, 0, &group, 0, 10)
        .await
        .unwrap();

    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs2[0].message.offset, 1);
}

#[tokio::test]
async fn cleanup_removes_old_acked_messages() {
    let store = make_test_store();
    let topic = "t".to_string();
    let group = "g".to_string();

    for i in 0..5 {
        store.append(&topic, 0, b"x").await.unwrap();
        store.mark_inflight(&topic, 0, &group, i, 0).await.unwrap();
        store.ack(&topic, 0, &group, i).await.unwrap();
    }

    store.cleanup_topic(&topic, 0).await.unwrap();

    let msgs = store.fetch_available(&topic, 0, &group, 0, 10).await.unwrap();
    assert!(msgs.is_empty());
}

#[tokio::test]
async fn cleanup_blocked_by_other_group() {
    let store = make_test_store();
    let topic = "t".to_string();

    let g1 = "g1".to_string();
    let g2 = "g2".to_string();

    // messages 0..4
    for _i in 0..5 {
        store.append(&topic, 0, b"x").await.unwrap();
    }

    // Group1 acks all
    for i in 0..5 {
        store.mark_inflight(&topic, 0, &g1, i, 0).await.unwrap();
        store.ack(&topic, 0, &g1, i).await.unwrap();
    }

    // Group2 has not consumed anything
    // → cleanup should not delete any messages
    store.cleanup_topic(&topic, 0).await.unwrap();

    let msgs = store.fetch_available(&topic, 0, &g2, 0, 10).await.unwrap();
    assert_eq!(msgs.len(), 5);
}

#[tokio::test]
async fn cleanup_preserves_remaining_offsets() {
    let store = make_test_store();
    let topic = "t".to_string();
    let g = "g".to_string();

    for _i in 0..10 {
        store.append(&topic, 0, b"x").await.unwrap();
    }

    // Ack first 7
    for i in 0..7 {
        store.mark_inflight(&topic, 0, &g, i, 0).await.unwrap();
        store.ack(&topic, 0, &g, i).await.unwrap();
    }

    store.cleanup_topic(&topic, 0).await.unwrap();

    let msgs = store.fetch_available(&topic, 0, &g, 0, 10).await.unwrap();
    let offsets: Vec<_> = msgs.iter().map(|m| m.message.offset).collect();

    assert_eq!(offsets, vec![7, 8, 9]);
}

#[tokio::test]
async fn append_batch_basic() {
    let store = make_test_store();
    let topic = "t".to_string();

    let payloads = vec![
        b"a".to_vec(),
        b"b".to_vec(),
        b"c".to_vec(),
    ];

    let offsets = store.append_batch(&topic, 0, &payloads).await.unwrap();
    assert_eq!(offsets, vec![0, 1, 2]);

    let msgs = store.fetch_available(&topic, 0, &"g".to_string(), 0, 10)
        .await.unwrap();

    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0].message.payload, b"a");
    assert_eq!(msgs[1].message.payload, b"b");
    assert_eq!(msgs[2].message.payload, b"c");
}

#[tokio::test]
async fn append_and_batch_interleave() {
    let store = make_test_store();
    let topic = "t".to_string();

    let off1 = store.append(&topic, 0, b"x").await.unwrap();
    assert_eq!(off1, 0);

    let offs = store.append_batch(
        &topic, 0,
        &[b"a".to_vec(), b"b".to_vec()]
    ).await.unwrap();
    assert_eq!(offs, vec![1, 2]);

    let off2 = store.append(&topic, 0, b"z").await.unwrap();
    assert_eq!(off2, 3);

    let msgs = store.fetch_available(&topic, 0, &"g".to_string(), 0, 10)
        .await.unwrap();

    let payloads: Vec<_> = msgs.into_iter()
        .map(|m| m.message.payload)
        .collect();

    assert_eq!(payloads, vec![b"x".to_vec(), b"a".to_vec(), b"b".to_vec(), b"z".to_vec()]);
}

#[tokio::test]
async fn append_batch_empty() {
    let store = make_test_store();
    let topic = "t".to_string();

    let offs = store.append_batch(&topic, 0, &[]).await.unwrap();
    assert!(offs.is_empty());
}
