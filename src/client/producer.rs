use bytes::Bytes;
use kafka_protocol::{
    messages::{
        ApiKey, ProduceRequest, RequestHeader, TopicName,
        produce_request::{PartitionProduceData, TopicProduceData},
    },
    protocol::StrBytes,
};

use super::KafkaClient;

impl KafkaClient {
    pub async fn request_produce(&self) {
        let api_key: i16 = ApiKey::Produce as _;
        let api_version = self.get_api_version(api_key);
        let header = RequestHeader::default()
            .with_request_api_key(api_key)
            .with_request_api_version(api_version.max_version);
        // .with_client_id(None);
        let body = ProduceRequest::default()
            .with_transactional_id(None)
            .with_acks(self.producer.required_acks as _)
            .with_timeout_ms(self.producer.timeout.as_millis() as _)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(TopicName::from(StrBytes::from_static_str("abc")))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(Bytes::new())),
                    ]),
            ]);
    }
    // pub async fn init_produce_loop(&self) {
    //     self.produce_loop.borrow_mut().get_or_insert(
    //         spawn_local(async move {
    //             TimerActionRepeat::
    //         }).detach()
    //     );
    // }
    pub async fn produce_record(&self) {
        let state = self.state.as_ref().unwrap().borrow();
        println!("{:#?}\n{:#?}", state.brokers, state.topics);
    }
}
