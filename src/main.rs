use tokio::net::{TcpSocket};

mod rdis;
use rdis::types::*;
use std::sync::Arc;
use std::ops::Deref;

#[tokio::main]
async fn main() -> ResultT<()> {
    let addr = "127.0.0.1:6379".parse().unwrap();
    let socket = TcpSocket::new_v4()?;

    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    println!("Bound socket to addr {}", addr);

    let listener = socket.listen(1024)?;

    let server = RedisServer::new(listener);
    let mut engine = RedisEngine::new();
    let api = Arc::new(RedisEngineApi::new(&engine));

    let server_handle = tokio::spawn(async move {engine.start_loop().await});

    while let Ok((stream, _)) = server.listener.accept().await {
        server.add_handle(tokio::spawn(server.client_connection(api.clone(), stream).start_loop()));
    }
    Ok(())
}




