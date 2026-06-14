use std::{fs::{File, OpenOptions, create_dir_all}, io::{Read, Seek, SeekFrom, Write}};

use crate::broker::Message;

#[derive(Debug)]
pub struct LogStorage {
    pub file: File,
}

impl LogStorage {
    pub fn new(path: &str) -> Self {
        create_dir_all("data").unwrap();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .unwrap();
        Self {
            file
        }
    }
    pub fn append(&mut self, message: &Message) {
        let line = format!("{}|{}", message.offset,message.payload);
        self.file.write(line.as_bytes()).unwrap();
        self.file.flush().unwrap();
    }
    pub fn load(&mut self) -> Vec<Message> {
        let mut v = Vec::new();
        let mut messages = Vec::new();
        let mut count = 0;
        self.file.seek(SeekFrom::Start(0));
        self.file.read_to_end(&mut v);
        for i in 0..v.len() {
            if v[i] == b'\n' {
                let s = str::from_utf8(&v[count..i]).unwrap();
                count = i + 1;
                println!("{:?}",s);
                let (offset, payload) = s.split_once("|").unwrap();
                messages.push(Message {
                    offset: offset.parse().unwrap(),
                    payload: payload.to_string()
                });
            }
        };
        messages
    }
}
