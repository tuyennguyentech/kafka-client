use std::{
    cell::RefCell,
    collections::HashMap,
    fs::ReadDir,
    hash::Hash,
    rc::Rc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures_lite::stream;
use glommio::{
    channels::local_channel::{self, LocalReceiver, LocalSender},
    enclose,
    timer::TimerActionOnce,
};
use kafka_protocol::{
    indexmap::IndexMap,
    messages::{
        ApiKey, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader, TopicName,
        produce_request::{PartitionProduceData, TopicProduceData},
    },
    protocol::{Decodable, HeaderVersion, Message, StrBytes, buf::ByteBufMut},
    records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType},
};
use murmur2::KAFKA_SEED;
use rand::{random, rngs::mock};

use crate::config::Producer;

use super::{KafkaClient, ProduceResult, RecordQueue, TIMEOUT, broker::Broker};

impl KafkaClient {
    async fn request_produce(
        broker: &mut Broker,
        producer: Producer,
        api_version: i16,
        mut topic_queues: HashMap<
            String,
            HashMap<i32, Vec<(Record, Rc<LocalSender<ProduceResult>>)>>,
        >,
    ) {
        // println!("request produce to {:?}:\n{:?}", broker.broker_type, topic_queues);
        let api_key: i16 = ApiKey::Produce as _;
        let header = RequestHeader::default()
            .with_request_api_key(api_key)
            .with_request_api_version(api_version);
        // .with_client_id(None);
        let mut sender_map: HashMap<String, HashMap<i32, Vec<Rc<LocalSender<ProduceResult>>>>> =
            HashMap::with_capacity(topic_queues.len());
        let topic_data = topic_queues
            .drain()
            .map(|(topic, mut partition_queues)| {
                TopicProduceData::default()
                    .with_name(TopicName(StrBytes::from_string(topic.clone())))
                    .with_partition_data(
                        partition_queues
                            .drain()
                            .map(|(partition_index, records)| {
                                let (records, senders): (Vec<_>, Vec<_>) =
                                    records.into_iter().unzip();
                                sender_map
                                    .entry(topic.clone())
                                    .or_default()
                                    .entry(partition_index)
                                    .or_insert(senders);
                                PartitionProduceData::default()
                                    .with_index(partition_index)
                                    .with_records(Some({
                                        let mut buf = BytesMut::new();
                                        RecordBatchEncoder::encode(
                                            &mut buf,
                                            records.iter(),
                                            &RecordEncodeOptions {
                                                version: 2,
                                                compression: Compression::Snappy,
                                            },
                                        )
                                        .unwrap();
                                        buf.freeze()
                                    }))
                            })
                            .collect(),
                    )
            })
            .collect();
        let body = ProduceRequest::default()
            .with_transactional_id(None)
            .with_acks(producer.required_acks as _)
            .with_timeout_ms(producer.timeout.as_millis() as _)
            .with_topic_data(topic_data);
        if let Some(mut buf) = broker.request(header, body).await {
            let _header =
                ResponseHeader::decode(&mut buf, ProduceResponse::header_version(api_version))
                    .unwrap();
            let body = ProduceResponse::decode(&mut buf, api_version).unwrap();
            // println!("{:#?}\n{:#?}", _header, body);
            for topic_produce_res in body.responses {
                let topic = topic_produce_res.name.to_string().clone();
                for partition_produce_res in topic_produce_res.partition_responses {
                    let partition_index = partition_produce_res.index;
                    stream::iter(sender_map.get(&topic).unwrap().get(&partition_index).unwrap()
                    .into_iter()
                    .enumerate()).for_each(|(i, sender)| async move {
                        sender.send(Ok((partition_index, partition_produce_res.base_offset + i as i64))).await.unwrap();
                    }).await;
                    // sender_map.get(&topic).unwrap().get(&partition_index).unwrap()
                    //     .into_iter()
                    //     .enumerate()
                    //     .for_each(|(i, sender)| {
                    //         sender.send(Ok((partition_index, partition_produce_res.base_offset + i)).await.unwrap();
                    //     });
                }
            }
        } else {
            panic!("Cannot produce to cluster");
        }
    }
    async fn handle_records_queue(
        records_queue: Rc<RefCell<RecordQueue>>,
        records_queue_size: Rc<RefCell<i32>>,
        brokers: Rc<RefCell<HashMap<i32, Broker>>>,
        producer: Producer,
        api_version: i16,
    ) {
        let queues: Vec<(
            i32,
            HashMap<String, HashMap<i32, Vec<(Record, Rc<LocalSender<ProduceResult>>)>>>,
        )> = records_queue.borrow_mut().drain().collect();
        *records_queue_size.borrow_mut() = 0;
        for (node_id, mut topic_queues) in queues {
            topic_queues.iter_mut().for_each(|(_, partition_queues)| {
                partition_queues.iter_mut().for_each(|(_, records)| {
                    let mut cnt = 0;
                    records.iter_mut().for_each(|x| {
                        x.0.offset = cnt;
                        x.0.sequence = cnt as _;
                        cnt += 1;
                    });
                });
            });
            let mut brokers = brokers.borrow_mut();
            let broker = brokers.get_mut(&node_id).unwrap();
            Self::request_produce(broker, producer.clone(), api_version, topic_queues).await;
        }
    }
    fn choose_partition_index(&self, topic: &String, key: &Option<String>) -> i32 {
        let tmp = if let Some(key) = key {
            murmur2::murmur2(key.as_bytes(), KAFKA_SEED)
        } else {
            random()
        } as usize;
        let partitions = &self.get_topic_metadata(&topic).partitions;
        partitions[tmp % partitions.len()].partition_index
    }
    async fn add_record_to_queue(
        &self,
        topic: &str,
        partition_index: &i32,
        record: Record,
        s: Rc<LocalSender<ProduceResult>>,
    ) {
        println!("add record to queue");
        let leader_id = self.get_leader_id_of_topic_and_partition(&topic, partition_index);
        self.get_records_queue_mut()
            .entry(leader_id)
            .or_default()
            .entry(topic.to_string())
            .or_default()
            .entry(*partition_index)
            .or_default()
            .push((record, s));
        let size = self.get_records_queue_size();
        *size.borrow_mut() += 1;
        println!("current size = {}", *size.borrow());
        if *size.borrow() > 1 && *size.borrow() < self.producer.flush.messages {
            return;
        }
        let producer = self.producer.clone();
        let api_version = self
            .get_api_version(ApiKey::Produce as _)
            .max_version
            .min(ProduceRequest::VERSIONS.max);
        println!("queue size = {}", *size.borrow());
        if *size.borrow() == 1 {
            self.get_produce_event_loop_mut()
                .replace(TimerActionOnce::do_in(
                    self.producer.flush.frequency,
                    enclose!((
                        self.get_records_queue() => records_queue,
                        size => size,
                        self.get_brokers() => brokers,
                        self.producer.flush.frequency => frequency,
                    ) async move {
                        println!("Schedule handle queue after {:?}", frequency);
                        Self::handle_records_queue(records_queue, size, brokers, producer, api_version).await;
                    }),
                ))
                .map(|e| e.destroy());
            println!("after schedule");
        } else if *size.borrow() >= self.producer.flush.messages {
            self.get_produce_event_loop_mut().as_mut().map(|e| {
                e.destroy();
            });
            println!("Queue flush");
            Self::handle_records_queue(
                self.get_records_queue(),
                size.clone(),
                self.get_brokers(),
                producer,
                api_version,
            )
            .await;
        }
    }
    pub async fn produce_record(
        &self,
        topic: String,
        key: Option<String>,
        value: Option<String>,
    ) -> ProduceResult {
        let partition_index = self.choose_partition_index(&topic, &key);
        let record = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: self
                .get_partition_metadata(&topic, &partition_index)
                .leader_epoch,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: -1,
            sequence: -1,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as _,
            key: key.map(|x| Bytes::from(x)),
            value: value.map(|x| Bytes::from(x)),
            headers: IndexMap::new(),
        };
        let (s, r) = local_channel::new_bounded(1);
        let s = Rc::new(s);
        let timeout_handle = TimerActionOnce::do_in(
            TIMEOUT,
            enclose!((s) async move {
                s.send(Err(None)).await.unwrap();
            }),
        );
        self.add_record_to_queue(&topic, &partition_index, record, s)
            .await;
        println!("after add record");
        let res = r.recv().await.unwrap();
        println!("after recv");
        timeout_handle.destroy();
        println!("finish {:?}", res);
        res
        // self.records.borrow_mut().push(record);
    }
}
