pub mod api_versions;
pub mod broker;
pub mod metadata;
pub mod produce;

use std::{
    cell::{Ref, RefCell, RefMut},
    collections::HashMap,
    rc::Rc,
    time::Duration,
};

use broker::{Broker, BrokerType};
use bytes::{BufMut as _, Bytes, BytesMut};
use glommio::{channels::local_channel::LocalSender, timer::TimerActionOnce};
use kafka_protocol::{
    messages::{
        ApiVersionsRequest, ApiVersionsResponse, RequestHeader,
        api_versions_response::ApiVersion,
        metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
    },
    protocol::{Encodable, HeaderVersion as _, StrBytes},
    records::Record,
};

use crate::config::{Metadata, Producer};

type ProduceResult = Result<(i32, i64), Option<i16>>;
type RecordQueue =
    HashMap<i32, HashMap<String, HashMap<i32, Vec<(Record, Rc<LocalSender<ProduceResult>>)>>>>;

#[derive(Default, Debug)]
pub struct KafkaClient {
    pub metadata: Metadata,
    pub producer: Producer,
    hosts: Vec<String>,
    state: State,
    bootstrap_broker: Option<Broker>,
    brokers: Rc<RefCell<HashMap<i32, Broker>>>,
    records_queue_size: Rc<RefCell<i32>>,
    records_queue: Rc<RefCell<RecordQueue>>,
    produce_event_loop: RefCell<Option<TimerActionOnce<()>>>,
}

#[derive(Debug, Default)]
struct State {
    // counter: i32,
    api_versions: Option<ApiVersionsResponse>,
    api_versions_map: Option<HashMap<i16, ApiVersion>>,
    brokers: Option<HashMap<i32, MetadataResponseBroker>>,
    topics: Option<HashMap<StrBytes, MetadataResponseTopic>>,
}

static TIMEOUT: Duration = Duration::from_secs(5);

impl KafkaClient {
    fn get_produce_event_loop_ref(&self) -> Ref<'_, Option<TimerActionOnce<()>>> {
        self.produce_event_loop.borrow()
    }
    fn get_produce_event_loop_mut(&self) -> RefMut<'_, Option<TimerActionOnce<()>>> {
        self.produce_event_loop.borrow_mut()
    }
    fn get_state_ref(&self) -> &State {
        &self.state
    }
    fn get_state_mut(&mut self) -> &mut State {
        &mut self.state
    }
    fn get_records_queue_size(&self) -> Rc<RefCell<i32>> {
        self.records_queue_size.clone()
    }
    fn get_records_queue_size_ref(&self) -> Ref<'_, i32> {
        self.records_queue_size.borrow()
    }
    fn get_records_queue_size_mut(&self) -> RefMut<'_, i32> {
        self.records_queue_size.borrow_mut()
    }
    fn get_records_queue(&self) -> Rc<RefCell<RecordQueue>> {
        self.records_queue.clone()
    }
    fn get_records_queue_ref(&self) -> Ref<'_, RecordQueue> {
        self.records_queue.borrow()
    }
    fn get_records_queue_mut(&self) -> RefMut<'_, RecordQueue> {
        self.records_queue.borrow_mut()
    }
    fn get_brokers(&self) -> Rc<RefCell<HashMap<i32, Broker>>> {
        self.brokers.clone()
    }
    pub fn get_brokers_ref(&self) -> Ref<'_, HashMap<i32, Broker>> {
        self.brokers.borrow()
    }
    fn get_brokers_mut(&self) -> RefMut<'_, HashMap<i32, Broker>> {
        self.brokers.borrow_mut()
    }
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }
    pub fn with_producer(mut self, producer: Producer) -> Self {
        self.producer = producer;
        self
    }
    pub fn with_hosts(mut self, hosts: Vec<String>) -> Self {
        self.hosts = hosts;
        self
    }
    fn encode<T: Encodable>(header: RequestHeader, body: T) -> Bytes {
        let mut header_buf = BytesMut::new();
        header
            .encode(
                &mut header_buf,
                ApiVersionsRequest::header_version(header.request_api_version),
            )
            .unwrap();

        let mut body_buf = BytesMut::new();
        body.encode(&mut body_buf, header.request_api_version)
            .unwrap();

        let size = header_buf.len() + body_buf.len();
        let mut final_buf = BytesMut::with_capacity(4 + size);
        final_buf.put_i32(size as _); // length prefix
        final_buf.extend_from_slice(&header_buf);
        final_buf.extend_from_slice(&body_buf);
        final_buf.freeze()
    }
    pub async fn init(&mut self) -> Result<(), &str> {
        // init bootstrap broker
        self.bootstrap_broker = Some({
            let broker =
                Broker::default().with_broker_type(BrokerType::Bootstrap(self.hosts.clone()));
            broker
        });
        self.request_api_versions("kafka-client".to_string(), "0.0.1".to_string())
            .await;
        self.request_metadata().await;
        Ok(())
    }

    async fn request<T: Encodable>(&mut self, header: RequestHeader, body: T) -> Option<Bytes> {
        self.bootstrap_broker
            .as_mut()
            .unwrap()
            .request(header, body)
            .await
    }
    async fn request_broker<T: Encodable>(
        &mut self,
        node_id: i32,
        header: RequestHeader,
        body: T,
    ) -> Option<Bytes> {
        self.get_brokers_mut()
            .get_mut(&node_id)
            .unwrap()
            .request(header, body)
            .await
    }
}
