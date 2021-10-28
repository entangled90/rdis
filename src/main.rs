use crate::rdis::engine::RedisEngine;
use tokio::net::TcpSocket;

mod rdis;
use opentelemetry::global;
use opentelemetry_jaeger;
use rdis::types::*;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::*;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;


#[tokio::main(worker_threads = 4)]
async fn main() -> ResultT<()> {
    
    let addr = "127.0.0.1:6379".parse()?;
    let socket = TcpSocket::new_v4()?;
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("rdis")
        .install_simple()?;
    // Create a tracing layer with the configured tracer


    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let sub = Registry::default().with(telemetry);
    
    
    tracing::subscriber::set_global_default(sub).expect("setting tracing default failed");

    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;

    info!("Bound socket to addr {}", addr);

    let listener = socket.listen(1024 * 1024)?;

    let server = RedisServer::new(listener);
    let (sender, receiver) = mpsc::channel(4096);
    let api = Arc::new(RedisEngineApi::new(sender));

    let _server_handle = tokio::spawn(async move {
        let mut engine = RedisEngine::new(receiver);
        engine.start_loop().await
    });

    accept_connections(server, api).await;

    global::shutdown_tracer_provider(); // sending remaining spans
    Ok(())
}

async fn accept_connections(server: RedisServer, api: Arc<RedisEngineApi>) {
    while let Ok((stream, _)) = server.listener.accept().await {
        server.add_handle(tokio::spawn(
            server.client_connection(api.clone(), stream).start_loop(),
        ));
    }
}