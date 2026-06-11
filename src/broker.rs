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
    pub topics: HashMap<String, Topic>
}

impl Broker {
    pub fn new() -> Self {
        Self {
            topics: HashMap::<String, Topic>::new()
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
    }
    pub fn publish(&mut self, topic: &str, payload: String) {
        let v = self.topics.get_mut(topic).unwrap();
        let logs = &mut v.storage;
        let s=  match v.messages.last(){
            Some(m) => {
                m.offset
            }
            None => {
                0
            }
        };
        let new = Message {
            offset: s+1,
            payload
        };
        v.messages.push(new.clone());
        logs.append(&new);
    }
    pub fn consume(&self, offset: u64, topic: &str) -> Vec<Message> {
        let v = &self.topics.get(topic).unwrap().messages;
        if offset as usize >= v.len() {
            return Vec::new()
        }
        v[offset as usize..].to_vec()
    }
    pub fn remake(&mut self) {
        for i in &mut self.topics {
            let s = i.1.storage.load();
            i.1.messages = s;
        }
    }
}