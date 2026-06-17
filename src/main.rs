use std::sync::Arc;
use bytes::BytesMut;
use tokio::{ net::{TcpListener, TcpStream}, sync::Mutex};

use crate::{read_handler::{process_request, read_command}, types::Broker, write_handler::write_command};

pub mod broker;
pub mod storage;
pub mod read_handler;
pub mod types;
pub mod write_handler;

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
        let s = read_command(&mut socket, &mut buf).await;
        match s {
            Some(value) => {
                let s = process_request(value.clone(),broker.clone()).await;
                write_command(&mut socket, value, s).await;
            },
            None => {
                println!("Disconnected");
                break
            }
        }
    }
}
