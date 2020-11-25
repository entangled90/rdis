use crate::rdis::engine::RedisEngine;
use std::time::Duration;
use tokio::net::TcpSocket;

mod rdis;
use log::{info, LevelFilter};
use rdis::types::*;
use simple_logger::SimpleLogger;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;

#[tokio::main(worker_threads = 4)]
async fn main() -> ResultT<()> {
    let logger = SimpleLogger::new().with_level(LevelFilter::Info);
    logger.init()?;

    let addr = "127.0.0.1:6379".parse()?;
    let socket = TcpSocket::new_v4()?;

    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    info!("Bound socket to addr {}", addr);

    let listener = socket.listen(1024)?;

    let server = RedisServer::new(listener);
    let (sender, receiver) = mpsc::channel(4096);
    let api = Arc::new(RedisEngineApi::new(sender));

    let _server_handle = std::thread::spawn(move || {
        let mut engine = RedisEngine::new(receiver);
        engine.start_loop()
    });

    // tokio::time::timeout(Duration::from_secs(10), 
    accept_connections(server, api).await;

    Ok(())
}


async fn accept_connections(server: RedisServer, api: Arc<RedisEngineApi>) {
    while let Ok((stream, _)) = server.listener.accept().await {
        server.add_handle(tokio::spawn(
            server.client_connection(api.clone(), stream).start_loop(),
        ));
    }
}
