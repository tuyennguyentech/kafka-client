use std::{
    cell::{RefCell, UnsafeCell},
    rc::Rc,
    time::Duration,
    vec,
};

use bytes::{Buf, Bytes, buf};
use futures::{AsyncReadExt, AsyncWriteExt, FutureExt, join, select};
use futures_lite::io::split;
use glommio::{
    dbg_context, enclose, executor, net::TcpStream, spawn_local, sync::{RwLock, Semaphore}, timer::{Timer, TimerActionOnce}, yield_if_needed, LocalExecutorBuilder
};
use kafka_client::{
    client::KafkaClient,
    config::{Metadata, Producer},
};
use kafka_protocol::protocol::buf::ByteBuf;

fn main() {
    LocalExecutorBuilder::default()
        .spawn(|| async move {
            // let u = Rc::<RefCell<Option<TcpStream>>>::default();
            // let v = u.take();
            // return ;
            // let handle = LocalExecutorBuilder::default()
            //     .spawn(|| async move {
            //         let action = TimerActionOnce::do_in(Duration::from_millis(100), async move {
            //             println!("hello");
            //         });
            //         action.rearm_in(Duration::from_millis(200));
            //         action.join().await;
            //     })
            //     .unwrap();
            // handle.join().unwrap();
            // return;
            let mut kafka_client = KafkaClient::default()
                .with_metadata(Metadata::default().with_refresh_frequency(Duration::from_secs(10)))
                .with_producer(Producer::default())
                .with_hosts(vec!["localhost:9092".to_string()]);
            kafka_client.init().await.unwrap();
            kafka_client
                .request_api_versions("prokater".to_string(), "0.0.0".to_string())
                .await;
            // let mut b = Bytes::new();
            // b.try_get_bytes(size)
            // let stream = TcpStream::connect("localhost:12345").await.unwrap();
            // let stream = Rc::new(RwLock::new(stream));
            // // enclose!()
            // let sem = Rc::new(Semaphore::new(1));
            // spawn_local(enclose!((mut stream, sem) async move {
            //     sem.acquire(1).await.unwrap();
            //     let mut buf = [0u8; 4];
            //     stream.write().await.unwrap().read(&mut buf).await.unwrap();
            //     println!("0: {}", String::from_utf8_lossy(&buf));
            //     sem.signal(1);
            // })).detach();

            // spawn_local(enclose!((mut stream, sem) async move {
            //     // dbg_context!()
            //     sem.acquire(1).await.unwrap();
            //     let mut buf = [0u8; 4];
            //     stream.write().await.unwrap().read(&mut buf).await.unwrap();
            //     println!("1: {}", String::from_utf8_lossy(&buf));
            //     sem.signal(1);
            // })).detach();

            // sem.acquire(1).await.unwrap();
            // let mut buf_read = vec![];
            // let mut buf_write = vec![];
            // let tmp = stream.read_exact(&mut buf);
            // println!("{:?}", tmp);
            // let t = stream.read_to_end(&mut buf);
            // select!()
            // Box::pin(stream)
            // let (mut reader, mut writer) = split(stream);
            // // let stream_read = stream.clone();
            // let t0 = spawn_local(async move {
            //     loop {
            //         let mut buf = vec![0u8; 1000];
            //         let n = reader.read(&mut buf).await.unwrap();
            //         println!("Receive: {}", std::str::from_utf8(&buf[0..n]).unwrap());
            //         Timer::new(Duration::from_millis(500)).await;
            //     }
            // });
            // // let stream_write = stream.clone();
            // let t1 = spawn_local(async move {
            //     loop {
            //         let buf = "hello\n".as_bytes();
            //         writer.write_all(buf).await.unwrap();
            //         println!("Write success");
            //         Timer::new(Duration::from_millis(500)).await;
            //     }
            // });
            // join!(t0, t1);
            // println!("{:?}", t);
        })
        .unwrap()
        .join()
        .unwrap();
}
