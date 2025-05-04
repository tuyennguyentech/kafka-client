use std::{
    cell::{RefCell, UnsafeCell},
    rc::Rc,
    time::Duration,
    vec,
};

use bytes::{Buf, Bytes, BytesMut, buf};
use futures::{AsyncReadExt, AsyncWriteExt, FutureExt, join, select};
use futures_lite::{
    StreamExt,
    io::{Cursor, split},
    stream,
};
use glommio::{
    LocalExecutorBuilder, enclose,
    spawn_local,
    timer::Timer,
};
use kafka_client::{
    client::KafkaClient,
    config::{Metadata, Producer},
};

fn main() {
    LocalExecutorBuilder::default()
        .spawn(|| async move {
            // let (s, r) = local_channel::new_bounded(1);
            // let timer = TimerActionOnce::do_in(Duration::from_secs(1), async move {
            //     println!("timer");
            //     s.send(true).await.unwrap();
            // });
            // r.recv().await.unwrap();
            // println!("after schedule");
            // Timer::new(Duration::from_secs(2)).await;
            // return ;
            // let (s, r) = local_channel::new_bounded(1);
            // s.send(1).await.unwrap();
            // drop(s);
            // Timer::new(Duration::from_millis(500));
            // println!("{:?}", r.recv().await);
            // println!("{:?}", r.recv().await);
            // return ;
            // let mut cursor: Cursor<[u8; 4]> = Cursor::new([0, 1, 2, 3]);
            // let mut buf = BytesMut::zeroed(3);
            // cursor.read_exact(&mut buf).await.unwrap();
            // println!("{:?}", buf.as_ref());
            // return ;
            // let jh = spawn_local(async {
            //     loop {
            //         Timer::new(Duration::from_secs(1)).await;
            //         println!("In loop");

            //         yield_if_needed().await;
            //     }
            // });
            // drop(jh);
            // println!("After drop");
            // // println!("{:?}", jh);
            // Timer::new(Duration::from_secs(10)).await;
            // return ;
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
            stream::iter(messages.into_iter())
                .for_each(|(topic, key, value)| {
                    spawn_local(enclose!((kafka_client => kafka_client) async move {
                        let res = kafka_client.borrow().produce_record(topic.clone(), key.clone(), value.clone()).await;
                        println!("{:?} => {:?}", (topic, key, value), res);
                    }))
                    .detach();
                })
                .await;
            Timer::new(Duration::from_secs(3)).await;
            // kafka_client.handle_records_queue().await;
            // let mut b = Bytes::new();
            // b.try_get_bytes(size)
            // let stream = TcpStream::connect("localhost:12345").await.unwrap();
            // let stream = Rc::new(RwLoc`k::new(stream));
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
