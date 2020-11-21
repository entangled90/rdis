use std::sync::Arc;
use std::sync::Mutex;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;


use std::error::Error;

pub type ResultT<A> = Result<A, Box<dyn Error>>;


use super::protocol::*;

pub struct RedisServer {
    pub listener: TcpListener,
    data: Arc<RedisData>,
    open_handles: Mutex<Vec<JoinHandle<()>>>,
}

// contains the common data structures
pub struct RedisData {}



impl RedisServer {
    pub fn new(listener: TcpListener) -> RedisServer {
        RedisServer {
            listener,
            data: Arc::new(RedisData {}),
            open_handles: Mutex::new(Vec::with_capacity(182)),
        }
    }

    pub fn client_connection(&self, stream: TcpStream) -> ClientConnection {
        ClientConnection {
            data: self.data.clone(),
            cmd: RedisCmd::new(stream),
        }
    }

    pub fn add_handle(&self, handle: JoinHandle<()>) -> Option<()> {
        let mut lock = self.open_handles.lock().unwrap();
        (*lock).push(handle);
        Some(())
    }
}

pub struct ClientConnection {
    data: Arc<RedisData>,
    cmd: RedisCmd,
}

impl ClientConnection {
    pub async fn handle(self) -> () {
        ()
    }
}