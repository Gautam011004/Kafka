use bytes::BytesMut;

pub async fn parse_message(buf: &mut BytesMut) {
    let bytes = buf.split();
    let msg = String::from_utf8(bytes.to_vec()).unwrap();
    let split_msg: Vec<String> = msg.split("|").map(String::from).collect();
    
}

pub async fn process_publish(msg: String) {
    

}