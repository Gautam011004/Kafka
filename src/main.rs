use std::sync::Arc;

use bytes::BytesMut;
use tokio::{io::AsyncReadExt, net::{TcpListener, TcpStream}, sync::Mutex};

use crate::{broker::Broker, handler::parse_message};

pub mod broker;
pub mod storage;
pub mod handler;

#[tokio::main]
async fn main() {
    let broker = Arc::new(Mutex::new(Broker::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let(mut socket, s) = listener.accept().await.unwrap();
        let broker =  broker.clone();
        tokio::spawn(handle_connection(socket, broker));
    }
}

async fn handle_connection(mut socket: TcpStream, broker: Arc<Mutex<Broker>>) {
    let mut buf = BytesMut::with_capacity(512);
    loop {
        let n = socket.read_buf(&mut buf).await.unwrap();
        if n == 0 {
            println!("Disconnected");
            break;
        }
        parse_message(&mut buf).await;
    }
}
