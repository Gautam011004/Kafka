use std::{sync::Arc};

use anyhow::{Error, anyhow};
use bytes::BytesMut;
use tokio::{io::AsyncReadExt, net::TcpStream, sync::Mutex};

use crate::types::{Broker, Command, CommandResponse};


pub async fn read_command(socket: &mut TcpStream, mut buf: &mut BytesMut) -> Option<Command> {
    loop {
        if let Some(v) = buf.iter().position(|x| *x == b'n') {
            let res = parse_message(buf, v as u32).unwrap();
            return Some(res)
        };
        let n = socket.read_buf(&mut buf).await.unwrap();
        if n == 0 {
            return None
        };

        if buf.len() < 4 {
            continue;
        };

        let len  = String::from_utf8(buf[0..4].to_vec()).unwrap().parse::<u32>().unwrap();

        let _ = buf.split_to(4);

        if len < buf.len() as u32 {
            continue;
        }
        
        let cmd = parse_message(buf, len ).unwrap();
        return Some(cmd)
    }
}

pub fn parse_message(buf: &mut BytesMut, pos: u32) ->  Result<Command, Error> {
    let bytes = buf.split_to((pos) as usize);
    println!("parse msg");
    println!("{:?}", String::from_utf8(bytes.to_vec()).unwrap());
    let _deserialized: Command = match serde_json::from_slice(&bytes) {
        Ok(v) => {
            return Ok(v)
        },
        Err(_) => {
            return Err(anyhow!("Could not deserialize"))
        }
    };
}

pub async fn process_request(command: Command,  data: Arc<Mutex<Broker>>) -> Option<CommandResponse> {
    let mut broker = data.lock().await;
    match command {
        Command::Publish { topic, partition, payload } => {
            broker.publish(&topic, payload, partition);
            return None
        },
        Command::Consume { topic, partition, offset } => {
            let r = broker.consume(offset, &topic, partition);
            return Some(CommandResponse::Consumed { data: r })
        },
        Command::Commit { topic, consumer, offset }=> {
            broker.commit(topic, consumer, offset);
            return None
        }
    }
}
