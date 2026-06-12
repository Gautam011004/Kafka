use std::{clone, collections::HashMap};

use crate::storage::LogStorage;

#[derive(Clone, Debug)]
pub struct Message {
    pub offset: u64,
    pub payload: String
}

pub struct Topic {
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

impl Broker {
    pub fn new() -> Self {
        Self {
            topics: HashMap::<String, Topic>::new(),
            groups: HashMap::<String, ConsumerGroup>::new()
        }
    }
    pub fn create_topic(&mut self, name: &str) {
        let path = format!("data/{}.log", name);
        let storage = LogStorage::new(&path);
        let v = Topic{
            messages: Vec::new(),
            storage: storage
        };
        self.topics.insert(name.to_string(), v);
        self.topics.get_mut(name).unwrap().messages = self.topics.get_mut(name).unwrap().storage.load();
    }
    pub fn publish(&mut self, topic: &str, payload: String) {
        let v = self.topics.get_mut(topic).unwrap();
        let logs = &mut v.storage;
        let s =  match v.messages.last(){
            Some(m) => {
                m.offset + 1
            }
            None => {
                0
            }
        };
        let new = Message {
            offset: s,
            payload
        };
        v.messages.push(new.clone());
        logs.append(&new);
    }
    pub fn create_group(&mut self, topic: String) {
        let new = ConsumerGroup { 
            offsets: HashMap::new()
        };
        self.groups.entry(topic).or_insert_with(|| new);
    }
    pub fn consume(&self, offset: u64, topic: &str) -> Vec<Message> {
        let v = &self.topics.get(topic).unwrap().messages;
        if offset as usize >= v.len() {
            return Vec::new()
        }
        v[offset as usize..].to_vec()
    }
    pub fn commit(&mut self, topic: String, consumer: String, commit: u64) {
        let v = self.groups.get_mut(&topic).unwrap();
        let s = v.offsets.entry(consumer).or_default();
        *s = commit;
    }
}