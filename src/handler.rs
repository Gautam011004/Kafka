use std::{sync::Arc};

use anyhow::{Error, anyhow};
use bytes::BytesMut;
use tokio::{io::AsyncReadExt, net::TcpStream, sync::Mutex};

use crate::types::{Broker, Command, Operation};


pub async fn read_command(mut socket: TcpStream, buf: &mut BytesMut) -> Option<Command> {
    loop {
        
        socket.read_buf(buf).await;
        if let Some(s) = buf.iter().position(|x| *x == b'\n') {
            let res = parse_message(buf, s).unwrap();
            return Some(res)
        }
    };
}

pub fn parse_message(buf: &mut BytesMut, pos: usize) ->  Result<Command, Error> {
    let bytes = buf.split_to(pos);
    let _deserialized: Command = match serde_json::from_slice(&bytes) {
        Ok(v) => {
            return Ok(v)
        },
        Err(_) => {
            return Err(anyhow!("Could not deserialize"))
        }
    };
}

pub async fn process_request(msg: Vec<String>, data: Arc<Mutex<Broker>>, operation: Operation) {
    let (topic, payload, mut id) = (msg[0].clone(), msg[1].clone(), msg[2].clone());
    println!("{:?}", id.pop().unwrap());
    let id = id.pop().unwrap().to_string().parse::<u32>().unwrap();
    let mut broker = data.lock().await;
    match operation {
        Operation::Publish => {
            println!("{},{}",topic, payload);
            broker.publish(&topic, payload, id);
        },
        Operation::Consume => {
            broker.consume(payload.parse::<u64>().unwrap(), &topic, id);
        },
        Operation::Commit => {
            let commit = msg[2].clone();
            broker.commit(topic, payload, commit.parse::<u64>().unwrap());
        }
    }
}
