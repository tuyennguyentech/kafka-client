mod producer;

use std::{
    cell::RefCell, collections::HashMap, rc::Rc, time::Duration
};

use bytes::{Buf, BufMut as _, Bytes, BytesMut};
use futures::AsyncReadExt;
use futures_lite::{
    AsyncWriteExt,
    io::{ReadHalf, WriteHalf, split},
};
use glommio::{
    channels::local_channel::{self, LocalSender},
    enclose, net::{Preallocated, TcpStream},
    spawn_local,
    sync::RwLock,
    task::JoinHandle,
    timer::TimerActionOnce,
};
use kafka_protocol::{
    messages::{
        api_versions_response::ApiVersion, ApiKey, ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader
    },
    protocol::{Decodable, Encodable, HeaderVersion as _, Message, StrBytes},
};

use crate::config::{Metadata, Producer};

#[derive(Default, Debug)]
pub struct KafkaClient {
    pub metadata: Metadata,
    pub producer: Producer,
    hosts: Vec<String>,
    reader: Option<Rc<RwLock<ReadHalf<TcpStream<Preallocated>>>>>,
    writer: Option<Rc<RwLock<WriteHalf<TcpStream<Preallocated>>>>>,
    inner_task: Option<JoinHandle<()>>,
    state: Option<RefCell<State>>,
    map: Rc<RefCell<HashMap<i32, LocalSender<Bytes>>>>,
}

#[derive(Debug, Default)]
struct State {
    counter: i32,
}

impl KafkaClient {
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
        if self.reader.is_some() || self.writer.is_some() {
            return Err("reader and writer are already initialized");
        }
        for host in self.hosts.iter() {
            if let Ok(stream) = TcpStream::connect(host).await {
                let (reader, writer) = split(stream.buffered());
                self.reader = Some(Rc::new(RwLock::new(reader)));
                self.writer = Some(Rc::new(RwLock::new(writer)));
                break;
            } else {
                eprintln!("Cannot connect to {}", host);
            }
        }
        if self.reader.is_none() || self.writer.is_none() {
            return Err("Cannot connect to any hosts");
        }
        self.state.replace(RefCell::new(State::default()));
        self.inner_task = Some(spawn_local(
            enclose!((self.reader.as_ref().unwrap() => mut reader, self.writer.as_ref().unwrap() => mut writer, self.map => mut map) async move {
                loop {
                    let size = {
                        let mut buf = [0u8; 4];
                        reader.write().await.unwrap().read_exact(&mut buf).await.unwrap();
                        i32::from_be_bytes(buf)
                    };
                    let mut buf = vec![0u8; size as _];
                    // println!("size = {}\nbuf={:?}", size, buf);
                    reader.write().await.unwrap().read_exact(&mut buf).await.unwrap();
                    let correlation_id = Bytes::copy_from_slice(&buf[0..4]).get_i32();
                    if let Some(sender) = map.borrow_mut().remove(&correlation_id) {
                        sender.send(Bytes::from_owner(buf)).await.unwrap();
                    };
                }
            }),
        ).detach());
        Ok(())
    }
    fn get_next_correlation_id(&self) -> i32 {
        let state = self.state.as_ref().unwrap();
        let mut state = state.borrow_mut();
        let correlation_id = state.counter;
        state.counter += 1;
        dbg!(correlation_id);
        correlation_id
    }
    async fn write_all_to_stream(&self, buf: &[u8]) {
        self.writer
            .as_ref()
            .unwrap()
            .write()
            .await
            .unwrap()
            .write_all(buf)
            .await
            .unwrap()
    }
    async fn request<T: Encodable>(&self, header: RequestHeader, body: T) -> Option<Bytes> {
        let correlation_id = self.get_next_correlation_id();
        let header = header.with_correlation_id(correlation_id);
        let buf = Self::encode(header, body);
        let (sender, receiver) = local_channel::new_bounded(1);
        self.map.borrow_mut().insert(correlation_id, sender);
        self.write_all_to_stream(&buf).await;
        let timer = TimerActionOnce::do_in(
            Duration::from_secs(5),
            enclose!((self.map => mut map) async move {
                map.borrow_mut().remove(&correlation_id).map(|sender: LocalSender<Bytes>| {
                    drop(sender);
                });
            }),
        );
        let res = receiver.recv().await;
        if res.is_some() {
            timer.destroy()
        }
        res
    }
    pub async fn request_api_versions(
        &self,
        client_software_name: String,
        client_software_version: String,
    ) {
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::ApiVersions as _)
            .with_request_api_version(ApiVersion::VERSIONS.max)
            .with_client_id(None);
        let body = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_string(client_software_name))
            .with_client_software_version(StrBytes::from_string(client_software_version));
        let res = self.request(header, body).await;
        if let Some(mut buf) = res {
            println!("size = {}\n{:?}", buf.len(), buf);
            let header = ResponseHeader::decode(&mut buf, ApiVersionsResponse::header_version(ApiVersionsResponse::VERSIONS.max)).unwrap();
            println!("{:?}", header);
            let body = ApiVersionsResponse::decode(&mut buf, ApiVersionsRequest::VERSIONS.max).unwrap();
            println!("{:#?}\n{:#?}", header, body);
        }
    }
}
