use std::sync::Arc;
use std::sync::Mutex;

use std::error::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub type ResultT<A> = Result<A, Box<dyn Error + Sync + Send>>;

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

    pub fn client_connection(
        &self,
        engine: Arc<RedisEngineApi>,
        stream: TcpStream,
    ) -> ClientConnection {
        ClientConnection {
            redis_cmd: RedisCmd::new(stream),
            engine,
        }
    }

    pub fn add_handle(&self, handle: JoinHandle<()>) -> Option<()> {
        let mut lock = self.open_handles.lock().unwrap();
        (*lock).push(handle);
        Some(())
    }
}

pub struct RedisEngine {
    data: RedisData,
    sender: mpsc::Sender<(RESP, oneshot::Sender<RESP>)>,
    receiver: mpsc::Receiver<(RESP, oneshot::Sender<RESP>)>,
}

impl RedisEngine {
    pub fn new() -> RedisEngine {
        let (sender, receiver) = mpsc::channel(4096*8);
        let data = RedisData {};
        RedisEngine {
            data,
            sender,
            receiver,
        }
    }

    pub async fn start_loop(&mut self) -> () {
        loop {
            match self.receiver.recv().await {
                Some((req, channel)) => channel.send(self.handle_request(req)).unwrap(),
                None => {
                    // TODO stay alive
                    println!("No senders, loop terminated");
                }
            }
        }
    }


    fn handle_request(&mut self, req: RESP) -> RESP{

        RESP::SimpleString("PING".to_owned())
    }
}


pub struct RedisEngineApi{
    sender: mpsc::Sender<(RESP, oneshot::Sender<RESP>)>,
}

impl RedisEngineApi{

    pub fn new (engine: &RedisEngine) -> RedisEngineApi{
        RedisEngineApi{
            sender: engine.sender.clone()
        }
    }

    pub async fn request(&self, req: RESP) -> ResultT<RESP> {
        let (tx, rx) = oneshot::channel();
        // fix this
        self.sender.send((req, tx)).await.unwrap();
        match rx.await {
            Ok(e) => Ok(e),
            Err(err) => Err(Box::new(err)),
        }
    }
}

pub struct ClientConnection {
    redis_cmd: RedisCmd,
    engine: Arc<RedisEngineApi>,
}

impl ClientConnection {
    pub async fn start_loop(mut self) -> () {
        println!("Connection received");
        loop {
            let cmd = (&mut self).redis_cmd.read_async().await;
            match cmd {
                Ok(Some(command)) => {
                    // println!("Received command {:?}", command);
                    let resp = match self.engine.request(command).await {
                        Ok(resp) => resp,
                        Err(err) => RESP::Error("Unexpected".to_owned(), err.to_string()),
                    };
                    // println!("Response is {:?}", resp);
                    match self.redis_cmd.write_async(resp).await {
                        Ok(()) => (),
                        Err(err) => {
                            println!("Erro when writing to client {}", err);
                            break;
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    eprintln!("Stopping loop, received error {}", err);
                    break;
                }
            }
        }
    }
}
