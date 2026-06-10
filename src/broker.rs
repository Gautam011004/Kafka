use std::{clone, collections::HashMap};

#[derive(Clone)]
pub struct Message {
    pub offset: u64,
    pub payload: String
}

pub struct Topic {
    pub messages: Vec<Message>
}

pub struct Broker {
    pub topics: HashMap<String, Topic>
}

impl Broker {
    fn new() -> Self {
        Self {
            topics: HashMap::<String, Topic>::new()
        }
    }
    fn create_topic(&mut self, name: &str) {
        let v = Topic{
            messages: Vec::new()
        };
        self.topics.insert(name.to_string(), v);
    }
    fn publish(&mut self, topic: &str, payload: String) {
        let v = self.topics.get_mut(topic).unwrap();
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
        v.messages.push(new);
    }
    fn consume(&self, offset: u64, topic: &str) -> Vec<Message> {
        let v = &self.topics.get(topic).unwrap().messages;
        if offset as usize >= v.len() {
            return Vec::new()
        }
        v[offset as usize..].to_vec()
    }
}