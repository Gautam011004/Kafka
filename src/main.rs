use crate::broker::Broker;

pub mod broker;
pub mod storage;
fn main() {
    let mut v = Broker::new();
    v.create_topic("orders");
    v.create_topic("transactions");
    v.create_topic("liquidation");
    v.create_group("orders".to_string());
    v.publish("orders", "BUY 1 BTC".to_string());
    v.publish("orders", "BUY 2 BTC".to_string());
    v.publish("orders", "SELL 1 BTC".to_string());
    v.publish("liquidation", "1 BTC DONE".to_string());
    v.publish("liquidation", "2 BTC DONE".to_string());
    v.commit("orders".to_string(), "analytics".to_string(), 2);
    println!("{:?}, \n", v.groups.get("orders").unwrap().offsets.get("analytics").unwrap());
}
