use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::types::{Command, CommandResponse};

pub async fn write_command(socket: &mut TcpStream, cmd: Command, data: Option<CommandResponse>) {
    let response =  match cmd {
        Command::Publish { topic, partition, payload } => {
            CommandResponse::Published { topic, partition, payload}
        },
        Command::Consume { topic, partition, offset } => {
            data.unwrap()
        }
        Command::Commit { topic, consumer, offset } => {
            CommandResponse::Commited { message: "Commited".to_string() }
        }
    };
    let mut bytes = serde_json::to_vec(&response).unwrap();
    bytes.push(b'\n');
    let _ = socket.write_all(&bytes).await;
}