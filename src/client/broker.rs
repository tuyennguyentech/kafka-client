use std::{cell::RefCell, collections::HashMap, rc::Rc};

use bytes::{Buf, Bytes, BytesMut};
use futures::AsyncReadExt;
use futures_lite::AsyncWriteExt;
use experiment::{ReadHalf, WriteHalf, split};
use glommio::{
    channels::local_channel::{self, LocalSender},
    enclose,
    net::{Preallocated, TcpStream},
    spawn_local,
    sync::RwLock,
    task::JoinHandle,
    timer::TimerActionOnce,
};
use kafka_protocol::{
    messages::{RequestHeader, metadata_response::MetadataResponseBroker},
    protocol::Encodable,
};

use super::{KafkaClient, TIMEOUT};

#[derive(Debug)]
pub enum BrokerType {
    Bootstrap(Vec<String>),
    Other(MetadataResponseBroker),
}

impl Default for BrokerType {
    fn default() -> Self {
        BrokerType::Bootstrap(vec![])
    }
}

#[derive(Default, Debug)]
pub struct Broker {
    pub broker_type: BrokerType,
    reader: Option<Rc<RwLock<ReadHalf<TcpStream<Preallocated>>>>>,
    writer: Option<Rc<RwLock<WriteHalf<TcpStream<Preallocated>>>>>,
    event_loop: Option<JoinHandle<()>>,
    map: Rc<RefCell<HashMap<i32, LocalSender<Bytes>>>>,
    correlation_id_cnt: i32,
}

// impl Default for Broker {
//     fn default() -> Self {
//         Self {
//             metadata: MetadataResponseBroker::default(),
//             reader: Default::default(),
//             writer: Default::default(),
//             event_loop: Default::default(),
//         }
//     }
// }

pub mod experiment {
    use std::{
        cell::RefCell,
        io::{IoSliceMut, Result},
        pin::Pin,
        rc::Rc,
        task::{Context, Poll},
    };

    use futures_io::{AsyncRead, AsyncWrite};

    pub fn split<T>(stream: T) -> (ReadHalf<T>, WriteHalf<T>)
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let inner = Rc::new(RefCell::new(stream));
        (ReadHalf(inner.clone()), WriteHalf(inner))
    }

    #[derive(Debug)]
    pub struct ReadHalf<T>(Rc<RefCell<T>>);

    #[derive(Debug)]
    pub struct WriteHalf<T>(Rc<RefCell<T>>);

    impl<T: AsyncRead + Unpin> AsyncRead for ReadHalf<T> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<Result<usize>> {
            let mut inner = self.0.borrow_mut();
            Pin::new(&mut *inner).poll_read(cx, buf)
        }

        fn poll_read_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &mut [IoSliceMut<'_>],
        ) -> Poll<Result<usize>> {
            let mut inner = self.0.borrow_mut();
            Pin::new(&mut *inner).poll_read_vectored(cx, bufs)
        }
    }

    impl<T: AsyncWrite + Unpin> AsyncWrite for WriteHalf<T> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize>> {
            let mut inner = self.0.borrow_mut();
            Pin::new(&mut *inner).poll_write(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
            let mut inner = self.0.borrow_mut();
            Pin::new(&mut *inner).poll_flush(cx)
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
            let mut inner = self.0.borrow_mut();
            Pin::new(&mut *inner).poll_close(cx)
        }
    }
}

impl Broker {
    pub fn with_broker_type(mut self, broker_type: BrokerType) -> Self {
        self.broker_type = broker_type;
        self
    }
    async fn connect(&mut self) -> TcpStream<Preallocated> {
        match &self.broker_type {
            BrokerType::Bootstrap(hosts) => {
                for host in hosts {
                    if let Ok(stream) = TcpStream::connect(host).await {
                        return stream.buffered();
                    }
                }
                panic!("Cannot connect to any hosts in {:?}", hosts);
            }
            BrokerType::Other(metadata) => {
                let addr = format!("{}:{}", metadata.host, metadata.port);
                TcpStream::connect(&addr)
                    .await
                    .expect(&format!("Broker {} should be reachable", addr))
                    .buffered()
            }
        }
    }
    pub async fn init(&mut self) {
        let stream = self.connect().await;
        let (r, w) = split(stream);
        self.reader.replace(Rc::new(RwLock::new(r)));
        self.writer.replace(Rc::new(RwLock::new(w)));
        if let Some(event_loop) = self.event_loop.replace(
            spawn_local(enclose!((
                self.reader.as_ref().unwrap() => mut reader,
                self.writer.as_ref().unwrap() => mut _writer,
                self.map => map
            ) async move {
                loop {
                    let size = {
                        let mut buf = [0u8; 4];
                        reader.write().await.unwrap().read_exact(&mut buf).await.unwrap();
                        i32::from_be_bytes(buf)
                    };
                    let mut buf = BytesMut::zeroed(size as _);
                    reader.write().await.unwrap().read_exact(&mut buf).await.unwrap();
                    let correlation_id = Bytes::copy_from_slice(&buf[0..4]).get_i32();
                    if let Some(sender) = map.borrow_mut().remove(&correlation_id) {
                        sender.send(buf.freeze()).await.unwrap();
                    }
                }
            }))
            .detach(),
        ) {
            event_loop.cancel();
        }
    }
    fn get_next_correlation_id(&mut self) -> i32 {
        let res = self.correlation_id_cnt;
        if self.correlation_id_cnt == i32::MAX {
            self.correlation_id_cnt = 0;
        } else {
            self.correlation_id_cnt += 1;
        }
        res
    }
    async fn write_all(&mut self, buf: &[u8]) {
        self.writer
            .as_ref()
            .unwrap()
            .write()
            .await
            .unwrap()
            .write_all(buf)
            .await
            .unwrap();
    }
    pub async fn request<T: Encodable>(&mut self, header: RequestHeader, body: T) -> Option<Bytes> {
        // check wheather event loop is started
        if self.event_loop.is_none() {
            self.init().await;
        }
        let c_id = self.get_next_correlation_id();
        let header = header.with_correlation_id(c_id);
        let buf = KafkaClient::encode(header, body);
        let (s, r) = local_channel::new_bounded(1);
        self.map.borrow_mut().insert(c_id, s);
        self.write_all(&buf).await;
        let timeout_handle = TimerActionOnce::do_in(
            TIMEOUT,
            enclose!(
                (self.map => map) async move {
                    map.borrow_mut().remove(&c_id)
                        .map(|s: LocalSender<Bytes>| {
                            drop(s);
                        });
                }
            ),
        );
        // I have test for case Sender is dropped after send something,
        // receiver still received item.
        let res = r.recv().await;
        if res.is_some() {
            timeout_handle.destroy();
        }
        res
    }
}
