use crate::broker::Broker;

pub mod broker;
pub mod storage;
fn main() {
    let mut v = Broker::new();
    v.create_topic("orders");
    v.create_topic("transactions");
    v.create_topic("liqidations");
    v.remake();
    let o = &v.topics.get("orders").unwrap().messages;
    let t = &v.topics.get("transactions").unwrap().messages;
    let l = &v.topics.get("liqidations").unwrap().messages;
    println!("{:?}, \n {:?}, \n {:?}", o, t, l);
}
