# Kafka Client

## What is Kafka Client?

Kafka Client is a crate provide a client which can communicate with Kafka Cluster base on Cooperative Thread-per-Core `async`/`await` architect. Kafka Client was built on top of `glommio` and `kafka_protocol`.

## Current supported requests types

- Produce
- Metadata
- ApiVersions

## Examples

``` rust
  let kafka_client = Rc::new(RefCell::new(
      KafkaClient::default()
          .with_metadata(
              Metadata::default().with_refresh_frequency(Duration::from_secs(10)),
          )
          .with_producer(Producer::default())
          .with_hosts(vec!["localhost:9092".to_string()]),
  )); // Create client with your own configuration
  // Create some messages
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

  // Concurrently produce messages
  stream::iter(messages.into_iter())
      .for_each(|(topic, key, value)| {
          spawn_local(enclose!((kafka_client => kafka_client) async move {
              let res = kafka_client.borrow().produce_record(topic.clone(), key.clone(), value.clone()).await;
              println!("{:?} => {:?}", (topic, key, value), res);
          }))
          .detach();
      })
      .await;

  Timer::new(Duration::from_secs(3)).await; // Wait to see all records have sent
```

## TODO list

- Test the throughput of producing record concurrently.
