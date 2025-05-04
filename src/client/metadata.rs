use kafka_protocol::{
    messages::{
        ApiKey, MetadataRequest, MetadataResponse, RequestHeader,
        ResponseHeader,
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
    },
    protocol::{Decodable as _, HeaderVersion as _, Message, StrBytes},
};

use super::{
    KafkaClient,
    broker::{self, Broker},
};

impl KafkaClient {
    fn store_brokers(&mut self, brokers: Vec<MetadataResponseBroker>) {
        self.get_state_mut().brokers.replace(
            brokers
                .into_iter()
                .map(|broker| (i32::from(broker.node_id), broker))
                .collect(),
        );
    }
    fn store_topics(&mut self, topics: Vec<MetadataResponseTopic>) {
        self.get_state_mut().topics.replace(
            topics
                .into_iter()
                .map(|topic| (StrBytes::from(topic.name.clone().unwrap()), topic))
                .collect(),
        );
    }
    pub(in crate::client) fn get_topic_metadata(&self, topic: &str) -> &MetadataResponseTopic {
        self.get_state_ref()
            .topics
            .as_ref()
            .unwrap()
            .get(&StrBytes::from_string(topic.to_string()))
            .unwrap()
    }
    pub(in crate::client) fn get_partition_metadata(
        &self,
        topic: &str,
        partition_index: &i32,
    ) -> &MetadataResponsePartition {
        self.get_topic_metadata(topic)
            .partitions
            .iter()
            .find(|partition| partition.partition_index == *partition_index)
            .unwrap()
    }
    pub(in crate::client) fn get_leader_id_of_topic_and_partition(
        &self,
        topic: &str,
        partition_index: &i32,
    ) -> i32 {
        self.get_partition_metadata(topic, partition_index).leader_id.0
    }
    fn init_brokers(&mut self) {
        let brokers_metadata = self.get_state_ref().brokers.as_ref().unwrap();
        let mut brokers = self.get_brokers_mut();
        for (node_id, metadata) in brokers_metadata {
            brokers.insert(
                *node_id,
                Broker::default().with_broker_type(broker::BrokerType::Other(metadata.clone())),
            );
        }
    }
    pub async fn request_metadata(&mut self) {
        let api_key = ApiKey::Metadata as _;
        let api_version = self
            .get_api_version(api_key)
            .max_version
            .min(MetadataRequest::VERSIONS.max);
        // dbg!(api_version);
        let header = RequestHeader::default()
            .with_request_api_key(api_key)
            .with_request_api_version(api_version)
            .with_client_id(None);
        let body = MetadataRequest::default()
            .with_topics(None)
            .with_allow_auto_topic_creation(true);
        // println!("{:?}", api_version);
        let res = self.request(header, body).await;
        if let Some(mut res) = res {
            let _header =
                ResponseHeader::decode(&mut res, MetadataResponse::header_version(api_version))
                    .unwrap();
            let body = MetadataResponse::decode(&mut res, api_version).unwrap();
            println!("{:?}", _header);
            // println!("{:?}\n{:?}", _header, body);
            self.store_brokers(body.brokers);
            self.store_topics(body.topics);
            self.init_brokers();
            // println!("{:#?}\n{:#?}", body.brokers, body.topics);
        } else {
            panic!("Cannot fetch metadata of cluster");
        }
    }
}
