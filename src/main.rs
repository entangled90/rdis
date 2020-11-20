use tokio::net::{TcpSocket};

mod rdis;
use rdis::types::*;


#[tokio::main]
async fn main() -> ResultT<()> {
    let addr = "127.0.0.1:6379".parse().unwrap();
    let socket = TcpSocket::new_v4()?;

    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    println!("Bound socket to addr {}", addr);

    let listener = socket.listen(1024)?;

    let server = RedisServer::new(listener);
    while let Ok((stream, _)) = server.listener.accept().await {
        server.add_handle(tokio::spawn(server.client_connection(stream).handle()));
    }
    Ok(())
}




