use crate::rdis::engine::RedisEngine;
use tokio::net::TcpSocket;

mod rdis;
use log::{LevelFilter};
use rdis::types::*;
use simple_logger::SimpleLogger;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;
use std::{fs::File, io::BufWriter};
use tracing_flame::FlameSubscriber;
use tracing_subscriber::{registry::Registry, prelude::*, fmt};

#[tokio::main(worker_threads = 4)]
async fn main() -> ResultT<()> {
    let guard = setup_global_subscriber();
    let addr = "127.0.0.1:6379".parse()?;
    let socket = TcpSocket::new_v4()?;

    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    info!("Bound socket to addr {}", addr);

    let listener = socket.listen(1024)?;

    let server = RedisServer::new(listener);
    let (sender, receiver) = mpsc::channel(4096);
    let api = Arc::new(RedisEngineApi::new(sender));

    let _server_handle = tokio::spawn(async move {
        let mut engine = RedisEngine::new(receiver);
        engine.start_loop().await
    });

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


fn setup_global_subscriber() -> impl Drop {
    let fmt_subscriber = fmt::Subscriber::default();

    let (flame_subscriber, _guard) = FlameSubscriber::with_file("./tracing.folded").unwrap();

    let collector = Registry::default()
        .with(fmt_subscriber)
        .with(flame_subscriber);

    tracing::collect::set_global_default(collector).expect("Could not set global default");
    _guard
}