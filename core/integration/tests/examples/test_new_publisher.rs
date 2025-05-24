// temporary file, do not forget to delete!

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use iggy::clients::send_mode::{BackgroundConfig, BackpressureMode, SendMode};
use iggy::prelude::defaults::*;
use iggy::prelude::*;
use iggy::{clients::client::IggyClient, prelude::TcpClient};
use iggy_common::TcpClientConfig;
use integration::test_server::{IpAddrKind, TestServer};

#[tokio::test]
async fn test_new_publisher() {
    // new
    let mut server = TestServer::new(None, true, None, IpAddrKind::V4);
    server.start();

    let tcp_client_config = TcpClientConfig {
        server_address: server.get_raw_tcp_addr().unwrap(),
        ..TcpClientConfig::default()
    };
    let client = Box::new(TcpClient::create(Arc::new(tcp_client_config)).unwrap());
    let client = IggyClient::create(client, None, None);

    // setup
    client.connect().await.unwrap();
    let ping_result = client.ping().await;
    assert!(ping_result.is_ok(), "Failed to ping server");

    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client
        .create_stream("sample-stream", Some(1))
        .await
        .unwrap();
    client
        .create_topic(
            &1.try_into().unwrap(),
            "sample-topic",
            1,
            CompressionAlgorithm::default(),
            None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let mut producer = client
        .producer("1", "1")
        .unwrap()
        .batch_length(10)
        .send_mode(SendMode::Sync)
        // .send_mode(SendMode::Background(BackgroundConfig {
        //     max_in_flight: 10,
        //     in_flight_timeout: None,
        //     batch_size: None,
        //     failure_mode: BackpressureMode::Block,
        // }))
        .build();

    producer.init().await.unwrap();

    // produce
    let mut send_batches = 0;

    let batch_limit = 10000;
    let mut count = 0;

    let mut t = Vec::new();
    while send_batches < batch_limit {
        let start = Instant::now();

        let mut messages = Vec::new();
        for _ in 0..10 {
            let payload = Bytes::from(vec![0u8; 1024]);
            let msg = IggyMessage::builder()
                .payload(payload)
                .build()
                .unwrap();
            messages.push(msg);
            count += 1;
        }

        producer.send(messages).await.unwrap();
        send_batches += 1;

        let duration = start.elapsed().as_millis();
        t.push(duration);
    }

    // let mut consumer = client
    //     .consumer_group("some-consumer", "1", "1")
    //     .unwrap()
    //     .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
    //     .create_consumer_group_if_not_exists()
    //     .auto_join_consumer_group()
    //     .polling_strategy(PollingStrategy::next())
    //     .poll_interval(IggyDuration::from_str("1ms").unwrap())
    //     .batch_length(10)
    //     .build();

    // consumer.init().await.unwrap();

    // let mut consumed_batches = 0;
    // while let Some(message) = consumer.next().await {
    //     if consumed_batches >= batch_limit {
    //         break
    //     }

    //     if let Ok(message) = message {
    //         // println!("got message");
    //         consumed_batches += 1;
    //     } else if let Err(error) = message {
    //         panic!("{}", error.to_string());
    //     }
    // }

    let total: u128 = t.iter().sum();
    let avg = total as f64 / t.len() as f64;
    println!("Среднее время выполнения одного батча: {:.3} мс", avg);
}

// sync: Время выполнения: 13.937269459s
