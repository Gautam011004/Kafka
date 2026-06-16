use std::sync::Arc;

use bytes::BytesMut;
use tokio::{io::AsyncReadExt, net::{TcpListener, TcpStream}, sync::Mutex};

use crate::{handler::{parse_message, process_request}, types::Broker};

pub mod broker;
pub mod storage;
pub mod handler;
pub mod types;

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
            break;
        }
    }
}
