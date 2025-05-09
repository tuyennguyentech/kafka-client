use std::{
    cell::RefCell,
    rc::Rc,
    time::Duration,
};

use futures_lite::{
    StreamExt,
    stream,
};
use glommio::{
    LocalExecutorBuilder, enclose, spawn_local,
    sync::Gate,
};
use kafka_client::{
    client::KafkaClient,
    config::{Metadata, Producer},
};

fn main() {
    LocalExecutorBuilder::default()
        .spawn(|| async move {
            let kafka_client = Rc::new(RefCell::new(
                KafkaClient::default()
                    .with_metadata(
                        Metadata::default().with_refresh_frequency(Duration::from_secs(10)),
                    )
                    .with_producer(Producer::default())
                    .with_hosts(vec!["localhost:9092".to_string()]),
            ));
            kafka_client.borrow_mut().init().await.unwrap();
            println!("{:?}", kafka_client.borrow().get_brokers_ref());
            // kafka_client.request_metadata().await;
            let mut messages = vec![
                (
                    "tmp".to_string(),
                    Some("0".to_string()),
                    Some("b".to_string()),
                ),
                ("tmp".to_string(), None, Some("1".to_string())),
                ("tmp".to_string(), Some("2".to_string()), None),
                (
                    "test".to_string(),
                    Some("0".to_string()),
                    Some("b".to_string()),
                ),
                ("test".to_string(), None, Some("1".to_string())),
                ("test".to_string(), Some("2".to_string()), None),
            ];
            for _ in 0..3 {
                messages.extend_from_within(0..messages.len());
            }
            let gate = Gate::new();
            stream::iter(messages.into_iter())
                .for_each(|(topic, key, value)| {
                    spawn_local(enclose!((kafka_client => kafka_client, gate) async move {
                        let _pass = gate.enter();
                        let res = kafka_client.borrow().produce_record(topic.clone(), key.clone(), value.clone()).await;
                        println!("{:?} => {:?}", (topic, key, value), res);
                    }))
                    .detach();
                })
                .await;
            gate.close().await.unwrap();
        })
        .unwrap()
        .join()
        .unwrap();
}
