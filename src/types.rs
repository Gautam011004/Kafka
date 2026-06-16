use std::{collections::HashMap, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::storage::LogStorage;

#[derive(Clone, Debug)]
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

#[derive(Debug)]
pub enum Operation {
    Publish,
    Consume,
    Commit
}

impl FromStr for Operation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Publish" => Ok(Operation::Publish),
            "Consume" => Ok(Operation::Consume),
            "Commit"  => Ok(Operation::Commit),
            _ => Err(format!("Error while parsing"))
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum Command {
    Publish {
        topic: String,
        partition: u32,
        payload: String
    },
    Consume {
        topic: String,
        partition: u32,
        offset: u32
    },
    Commit {
        consumer: String,
        offset: u32
    }
}