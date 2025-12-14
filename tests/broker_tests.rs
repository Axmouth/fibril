use std::sync::Arc;

use thetube::broker::*;
use thetube::broker::coordination::*;
use thetube::storage::*;

// TODO: make shared
fn make_test_store() -> Box<dyn Storage> {
    // make testdata dir
    std::fs::create_dir_all("test_data").unwrap();
    // make random temp filename to avoid conflicts
    let filename = format!("test_data/{}", fastrand::u64(..));
    Box::new(make_rocksdb_store(&filename).unwrap())
}

fn make_test_broker() -> Broker<NoopCoordination> {
    let store = make_test_store();
    Broker::new(store, NoopCoordination, BrokerConfig { ttl: 3, batch_size: 10, batch_timeout_ms: 100 })
}

#[tokio::test]
async fn broker_basic_delivery() {
    let broker = make_test_broker();

    // Publish 3 messages
    broker.publish("t1", b"hello").await.unwrap();
    broker.publish("t1", b"world").await.unwrap();
    broker.publish("t1", b"!").await.unwrap();

    // Subscribe
    let mut consumer = broker.subscribe("t1", "g1").await.unwrap();

    let mut received = vec![];
    for _ in 0..3 {
        let msg = consumer.messages.recv().await.unwrap();
        received.push(String::from_utf8(msg.message.payload.clone()).unwrap());
        consumer.acker.send(msg.delivery_tag).await.unwrap();
    }

    assert_eq!(received, ["hello", "world", "!"]);
}

#[tokio::test]
async fn broker_ack_behavior() {
    let broker = make_test_broker();

    broker.publish("t", b"a").await.unwrap();
    broker.publish("t", b"b").await.unwrap();

    let mut consumer = broker.subscribe("t", "g").await.unwrap();

    // Receive & ack first message
    let m1 = consumer.messages.recv().await.unwrap();
    assert_eq!(m1.message.payload, b"a");
    consumer.acker.send(m1.delivery_tag).await.unwrap();

    // Receive & ack second
    let m2 = consumer.messages.recv().await.unwrap();
    assert_eq!(m2.message.payload, b"b");
    consumer.acker.send(m2.delivery_tag).await.unwrap();

    // Should not receive anything else now
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    assert!(consumer.messages.try_recv().is_err());
}

#[tokio::test]
async fn broker_work_queue_distribution() {
    let broker = make_test_broker();

    // Publish some work
    for i in 0..6 {
        broker.publish("jobs", format!("job-{i}").as_bytes()).await.unwrap();
    }

    // Two consumers, same group (competing consumers)
    let mut c1 = broker.subscribe("jobs", "workers").await.unwrap();
    let mut c2 = broker.subscribe("jobs", "workers").await.unwrap();

    let mut got1 = 0;
    let mut got2 = 0;

    // Collect 6 messages total (in any order)
    for _ in 0..6 {
        tokio::select! {
            Some(msg) = c1.messages.recv() => {
                got1 += 1;
                c1.acker.send(msg.delivery_tag).await.unwrap();
            }
            Some(msg) = c2.messages.recv() => {
                got2 += 1;
                c2.acker.send(msg.delivery_tag).await.unwrap();
            }
        }
    }

    // Work should be distributed, not replicated
    assert_eq!(got1 + got2, 6);
    assert!(got1 > 0);
    assert!(got2 > 0);
}

#[tokio::test]
async fn broker_pubsub_multiple_groups() {
    let broker = make_test_broker();

    broker.publish("events", b"alpha").await.unwrap();
    broker.publish("events", b"beta").await.unwrap();

    let mut g1 = broker.subscribe("events", "g1").await.unwrap();
    let mut g2 = broker.subscribe("events", "g2").await.unwrap();

    let mut recv_g1 = vec![];
    let mut recv_g2 = vec![];

    for _ in 0..2 {
        let m1 = g1.messages.recv().await.unwrap();
        recv_g1.push(String::from_utf8(m1.message.payload.clone()).unwrap());
        g1.acker.send(m1.delivery_tag).await.unwrap();

        let m2 = g2.messages.recv().await.unwrap();
        recv_g2.push(String::from_utf8(m2.message.payload.clone()).unwrap());
        g2.acker.send(m2.delivery_tag).await.unwrap();
    }

    assert_eq!(recv_g1, ["alpha", "beta"]);
    assert_eq!(recv_g2, ["alpha", "beta"]);
}

#[tokio::test]
async fn broker_delivery_in_order() {
    let broker = make_test_broker();

    for i in 0..10 {
        broker.publish("numbers", format!("{}", i).as_bytes()).await.unwrap();
    }

    let mut consumer = broker.subscribe("numbers", "g").await.unwrap();

    let mut collected = vec![];

    for _ in 0..10 {
        let msg = consumer.messages.recv().await.unwrap();
        collected.push(String::from_utf8(msg.message.payload.clone()).unwrap());
        consumer.acker.send(msg.delivery_tag).await.unwrap();
    }

    assert_eq!(collected, (0..10).map(|i| i.to_string()).collect::<Vec<_>>());
}

#[tokio::test]
async fn broker_consumer_drop_stops_delivery() {
    let broker = make_test_broker();

    broker.publish("t", b"x").await.unwrap();

    let consumer = broker.subscribe("t", "g").await.unwrap();

    // Drop receiver
    drop(consumer.messages);

    // Delivery loop should stop without panicking
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;

    // Publishing more messages should not cause issues
    assert!(broker.publish("t", b"y").await.is_ok());
}

#[tokio::test]
async fn redelivery_occurs_after_expiration() {
    let broker = make_test_broker();
    broker.start_redelivery_worker();

    // Publish 1 message
    broker.publish("topic", b"hello").await.unwrap();

    // Subscribe
    let mut cons = broker.subscribe("topic", "g").await.unwrap();

    // First delivery
    let m1 = cons.messages.recv().await.unwrap();
    assert_eq!(m1.message.payload, b"hello");

    // Do NOT ack → let it expire
    tokio::time::sleep(std::time::Duration::from_secs(4)).await; // >3s TTL

    // Message should be redelivered
    let m2 = cons.messages.recv().await.unwrap();
    assert_eq!(m2.message.offset, m1.message.offset);
    assert_eq!(m2.message.payload, b"hello");

    // Now ACK to finish
    cons.acker.send(m2.delivery_tag).await.unwrap();
}

#[tokio::test]
async fn ack_prevents_redelivery() {
    let broker = make_test_broker();
    broker.start_redelivery_worker();

    broker.publish("topic", b"hi").await.unwrap();

    let mut cons = broker.subscribe("topic", "g").await.unwrap();

    let msg = cons.messages.recv().await.unwrap();
    cons.acker.send(msg.delivery_tag).await.unwrap();

    // Wait past expiration window
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    // Should NOT redeliver
    assert!(cons.messages.try_recv().is_err());

    drop(cons);
    drop(broker);
}

#[tokio::test]
async fn redelivery_load_balanced_to_consumers() {
    let broker = make_test_broker();
    broker.start_redelivery_worker();

    broker.publish("t", b"x").await.unwrap();

    let mut c1 = broker.subscribe("t", "g").await.unwrap();
    let mut c2 = broker.subscribe("t", "g").await.unwrap();

    let m = c1.messages.recv().await.unwrap();
    assert_eq!(m.message.payload, b"x");

    // Let it expire
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    // Should go to any consumer — we just verify it redelivers
    let redelivered = tokio::select! {
        Some(m2) = c1.messages.recv() => m2,
        Some(m2) = c2.messages.recv() => m2,
    };

    assert_eq!(redelivered.message.payload, b"x");
}

#[tokio::test]
async fn selective_ack_out_of_order() {
    let broker = make_test_broker();
    broker.start_redelivery_worker();

    // Publish messages 0..4
    for i in 0..5 {
        broker.publish("t", format!("m{i}").as_bytes()).await.unwrap();
    }

    let mut cons = broker.subscribe("t", "g").await.unwrap();

    // Receive messages
    let mut messages = vec![];
    for _ in 0..5 {
        let msg = cons.messages.recv().await.unwrap();
        messages.push(msg);
    }

    // ACK out-of-order: ACK offset 3 and 4 first
    cons.acker.send(messages[3].delivery_tag).await.unwrap();
    cons.acker.send(messages[4].delivery_tag).await.unwrap();

    // ACK 0 next
    cons.acker.send(messages[0].delivery_tag).await.unwrap();

    // ACK 2 before 1
    cons.acker.send(messages[2].delivery_tag).await.unwrap();
    cons.acker.send(messages[1].delivery_tag).await.unwrap();

    // No redelivery should occur
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(cons.messages.try_recv().is_err());
}

#[tokio::test]
async fn selective_ack_expiry_redelivery() {
    let broker = make_test_broker();
    broker.start_redelivery_worker();

    for i in 0..5 {
        broker.publish("t", format!("v{i}").as_bytes()).await.unwrap();
    }

    let mut cons = broker.subscribe("t", "g").await.unwrap();

    // Receive all messages 0..4
    let mut msgs = vec![];
    for _ in 0..5 {
        msgs.push(cons.messages.recv().await.unwrap());
    }

    // ACK everything except offset 2
    for i in [0, 1, 3, 4] {
        cons.acker.send(msgs[i].delivery_tag).await.unwrap();
    }

    // Let inflight(2) expire
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Should re-deliver only offset 2
    let redelivered = cons.messages.recv().await.unwrap();
    assert_eq!(redelivered.message.offset, 2);

    // ACK it finally
    cons.acker.send(redelivered.delivery_tag).await.unwrap();

    // Should not redeliver again
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let next = cons.messages.try_recv();
    assert!(next.is_err());
}

#[tokio::test]
async fn selective_ack_no_wrong_rewind() {
    let broker = make_test_broker();
    broker.start_redelivery_worker();

    for i in 0..3 {
        broker.publish("t", format!("x{i}").as_bytes()).await.unwrap();
    }

    let mut cons = broker.subscribe("t", "g").await.unwrap();

    let m0 = cons.messages.recv().await.unwrap();
    let _m1 = cons.messages.recv().await.unwrap();
    let m2 = cons.messages.recv().await.unwrap();

    // ACK 2 then 0 (skipping 1)
    cons.acker.send(m2.delivery_tag).await.unwrap();
    cons.acker.send(m0.delivery_tag).await.unwrap();

    // No redelivery yet
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(cons.messages.try_recv().is_err());

    // Let 1 expire
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    // Should redeliver only offset=1
    let redelivered = cons.messages.recv().await.unwrap();
    assert_eq!(redelivered.message.offset, 1);
}
