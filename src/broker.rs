use std::{collections::HashMap};

use crate::{storage::LogStorage, types::{Broker, ConsumerGroup, Message, Partitions, Topic}};

impl Broker {
    pub fn new() -> Self {
        Self {
            topics: HashMap::<String, Topic>::new(),
            groups: HashMap::<String, ConsumerGroup>::new()
        }
    }
    pub fn create_topic(&mut self, name: &str, id: u32) {
        let path = format!("data/{}-{}.log", name,id);
        let storage = LogStorage::new(&path);
        let p = Partitions {
            id,
            messages: Vec::new(),
            storage
        };
        let mut v = Topic { partitions: Vec::new() };
        v.partitions.push(p);
        self.topics.insert(name.to_string(), v);
    }
    pub fn publish(&mut self, topic: &str, payload: String, id: u32) {
        let v = match self.topics.get_mut(topic) {
            Some(value) => {
                let s = match value.partitions.get_mut(id as usize) {
                    Some(v) => {
                        v.storage.load();
                        v
                    },
                    None => {
                        let path = format!("data/{}-{}.log", topic,id);
                        let storage = LogStorage::new(&path);
                        value.partitions.push(Partitions { id, messages: Vec::new(), storage });
                        let v = value.partitions.get_mut(id as usize).unwrap();
                        v.storage.load();
                        v
                    }
                };
                s
            },
            None => {
                self.create_topic(topic, id);
                let s = self.topics.get_mut(topic).unwrap();
                let v = s.partitions.get_mut(id as usize).unwrap();
                v.storage.load();
                v
            }
        };
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
    pub fn consume(&mut self, offset: u64, topic: &str, id: u32) -> Vec<Message> {
        let v = &self.topics.get_mut(topic).unwrap().partitions.get_mut(id as usize).unwrap().messages;
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