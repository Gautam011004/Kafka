use std::{collections::HashMap, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::storage::LogStorage;

#[derive(Clone, Debug, Serialize)]
pub struct Message {
    pub offset: u64,
    pub payload: String
}

pub struct Topic {
    pub partitions: Vec<Partitions>
}

pub struct Partitions {
    pub id: u32,
    pub messages: Vec<Message>,
    pub storage: LogStorage
}
pub struct Broker {
    pub topics: HashMap<String, Topic>,
    pub groups: HashMap<String, ConsumerGroup>
}

pub struct ConsumerGroup {
    pub offsets: HashMap<String, u64>
}


#[derive(Serialize, Deserialize, Clone)]
pub enum Command {
    Publish {
        topic: String,
        partition: u32,
        payload: String
    },
    Consume {
        topic: String,
        partition: u32,
        offset: u64
    },
    Commit {
        topic: String,
        consumer: String,
        offset: u64
    }
}

#[derive(Serialize)]
pub enum CommandResponse {
    Published {
        topic: String,
        partition: u32,
        payload: String
    },
    Consumed {
        data: Vec<Message>
    },
    Commited {
        message: String
    }
}