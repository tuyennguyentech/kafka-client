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
type RecordQueue = HashMap<i32, HashMap<String, HashMap<i32, Vec<(Record, Rc<LocalSender<ProduceResult>>)>>>>;

#[derive(Default, Debug)]
pub struct KafkaClient {
    pub metadata: Metadata,
    pub producer: Producer,
    hosts: Vec<String>,
    state: State,
    bootstrap_broker: Option<Broker>,
    brokers: Rc<RefCell<HashMap<i32, Broker>>>,
    records_queue_size: Rc<RefCell<i32>>,
    records_queue:
        Rc<RefCell<RecordQueue>>,
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
    fn get_records_queue(
        &self,
    ) -> Rc<RefCell<RecordQueue>>
    {
        self.records_queue.clone()
    }
    fn get_records_queue_ref(
        &self,
    ) -> Ref<'_, RecordQueue> {
        self.records_queue.borrow()
    }
    fn get_records_queue_mut(
        &self,
    ) -> RefMut<'_, RecordQueue>
    {
        self.records_queue.borrow_mut()
    }
    fn get_brokers(&self) -> Rc<RefCell<HashMap<i32, Broker>>> {
        self.brokers.clone()
    }
    fn get_brokers_ref(&self) -> Ref<'_, HashMap<i32, Broker>> {
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

        // if self.reader.is_some() || self.writer.is_some() {
        //     return Err("reader and writer are already initialized");
        // }
        // for host in self.hosts.iter() {
        //     if let Ok(stream) = TcpStream::connect(host).await {
        //         let (reader, writer) = split(stream.buffered());
        //         self.reader = Some(Rc::new(RwLock::new(reader)));
        //         self.writer = Some(Rc::new(RwLock::new(writer)));
        //         break;
        //     } else {
        //         eprintln!("Cannot connect to {}", host);
        //     }
        // }
        // if self.reader.is_none() || self.writer.is_none() {
        //     return Err("Cannot connect to any hosts");
        // }
        // self.state.replace(RefCell::new(State::default()));
        // self.inner_task = Some(spawn_local(
        //     enclose!((self.reader.as_ref().unwrap() => mut reader, self.writer.as_ref().unwrap() => mut _writer, self.map => mut map) async move {
        //         loop {
        //             let size = {
        //                 let mut buf = [0u8; 4];
        //                 reader.write().await.unwrap().read_exact(&mut buf).await.unwrap();
        //                 i32::from_be_bytes(buf)
        //             };
        //             let mut buf = vec![0u8; size as _];
        //             // println!("size = {}\nbuf={:?}", size, buf);
        //             reader.write().await.unwrap().read_exact(&mut buf).await.unwrap();
        //             let correlation_id = Bytes::copy_from_slice(&buf[0..4]).get_i32();
        //             if let Some(sender) = map.borrow_mut().remove(&correlation_id) {
        //                 sender.send(Bytes::from_owner(buf)).await.unwrap();
        //             };
        //         }
        //     }),
        // ).detach());
        // self.state.replace(State::default());
        self.request_api_versions("kafka-client".to_string(), "0.0.1".to_string())
            .await;
        self.request_metadata().await;
        Ok(())
    }
    // fn _get_next_correlation_id(&mut self) -> i32 {
    //     let state = self.state.as_ref().unwrap();
    //     let mut state = state.borrow_mut();
    //     let correlation_id = state.counter;
    //     state.counter += 1;
    //     // dbg!(correlation_id);
    //     correlation_id
    // }
    // async fn _write_all_to_stream(&mut self, buf: &[u8]) {
    //     self.writer
    //         .as_ref()
    //         .unwrap()
    //         .write()
    //         .await
    //         .unwrap()
    //         .write_all(buf)
    //         .await
    //         .unwrap()
    // }
    async fn request<T: Encodable>(&mut self, header: RequestHeader, body: T) -> Option<Bytes> {
        self.bootstrap_broker
            .as_mut()
            .unwrap()
            .request(header, body)
            .await
        // let correlation_id = self.get_next_correlation_id();
        // let header = header.with_correlation_id(correlation_id);
        // let buf = Self::encode(header, body);
        // let (sender, receiver) = local_channel::new_bounded(1);
        // self.map.borrow_mut().insert(correlation_id, sender);
        // self.write_all_to_stream(&buf).await;
        // let timer = TimerActionOnce::do_in(
        //     TIMEOUT,
        //     enclose!((self.map => mut map) async move {
        //         map.borrow_mut().remove(&correlation_id).map(|sender: LocalSender<Bytes>| {
        //             drop(sender);
        //         });
        //     }),
        // );
        // let res = receiver.recv().await;
        // if res.is_some() {
        //     timer.destroy()
        // }
        // res
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
