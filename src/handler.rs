use std::sync::Arc;

use bytes::BytesMut;
use tokio::sync::Mutex;

use crate::broker::{Broker, Operation};

pub async fn parse_message(buf: &mut BytesMut) ->  Vec<String> {
    let bytes = buf.split();
    let msg = String::from_utf8(bytes.to_vec()).unwrap();
    println!("{}",msg);
    let split_msg: Vec<String> = msg.split("|").map(String::from).collect();
    split_msg
}

pub async fn process_request(msg: Vec<String>, data: Arc<Mutex<Broker>>, operation: Operation) {
    let (topic, payload) = (msg[0].clone(), msg[1].clone());
    println!("{},{}",topic,payload);
    println!("{:?}",operation);
    let mut broker = data.lock().await;
    match operation {
        Operation::Publish => {
            println!("{},{}",topic, payload);
            broker.publish(&topic, payload);
        },
        Operation::Consume => {
            broker.consume(payload.parse::<u64>().unwrap(), &topic);
        },
        Operation::Commit => {
            let commit = msg[2].clone();
            broker.commit(topic, payload, commit.parse::<u64>().unwrap());
        }
    }
}
