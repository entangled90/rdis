use tokio::time::Instant;
use std::fmt::Formatter;
use std::fmt::Display;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::tcp::OwnedReadHalf;
use tokio::io::BufWriter;
use tokio::io::BufReader;
use std::sync::{Arc, Mutex, atomic:: {AtomicUsize, Ordering}};

use std::error::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use log::{info, debug, warn, error};

use super::engine::*;
use hdrhistogram::Histogram;

pub type ErrorT = Box<dyn Error + Sync + Send>;
pub type ResultT<A> = Result<A, ErrorT>;

use super::protocol::*;


pub struct RedisServer {
    pub listener: TcpListener,
    open_handles: Mutex<Vec<JoinHandle<()>>>,
    client_epoch: AtomicUsize
}


impl RedisServer {
    pub fn new(listener: TcpListener) -> RedisServer {
        
        RedisServer {
            listener,
            open_handles: Mutex::new(Vec::with_capacity(1024)),
            client_epoch: AtomicUsize::new(0),

        }
    }

    pub fn client_connection(
        &self,
        engine: Arc<RedisEngineApi>,
        stream: TcpStream,
    ) -> ClientConnection {
        let client_epoch = self.client_epoch.fetch_add(1, Ordering::SeqCst);
        ClientConnection {
            redis_cmd: RedisCmd::from_stream(stream, client_epoch),
            engine,
            client_epoch
        }
    }

    pub fn add_handle(&self, handle: JoinHandle<()>) -> Option<()> {
        let mut lock = self.open_handles.lock().unwrap();
        (*lock).push(handle);
        Some(())
    }
}


pub struct RedisEngineApi {
    sender: mpsc::Sender<(RESP, oneshot::Sender<RESP>)>,
}
impl RedisEngineApi {
    pub fn new(sender: mpsc::Sender<(RESP, oneshot::Sender<RESP>)> ) -> RedisEngineApi {
        RedisEngineApi {
            sender: sender.clone(),
        }
    }

    pub async fn request(&self, req: RESP) -> ResultT<RESP> {
        let (tx, rx) = oneshot::channel();
        // fix this
        self.sender.send((req, tx)).await.unwrap();
        match rx.await {
            Ok(e) => {
                Ok(e)
            },
            Err(err) => Err(Box::new(err)),
        }
    }
}

pub struct ClientConnection {
    redis_cmd: RedisCmd<OwnedReadHalf,BufWriter<OwnedWriteHalf>>,
    engine: Arc<RedisEngineApi>,
    client_epoch: usize
}


impl Display for ClientConnection{
    fn fmt(&self, f: &mut Formatter) -> std::result::Result<(), std::fmt::Error> {
        f.write_fmt(format_args!("ClientConnection{{client_epoch: {} }}", self.client_epoch))
    }
}

impl  ClientConnection {
    pub async fn start_loop(mut self) -> () {
        info!("Connection received {}", self);
        loop {
            let before_read = Instant::now();
            let cmd = (&mut self).redis_cmd.read_async().await;
            let read_delta = before_read.elapsed().as_micros();
            debug!("Time for read {}, client={}", read_delta, self.client_epoch);
            match cmd {
                Ok(mut commands) => if commands.len() > 0{
                    debug!("Received command {:?}", commands);
                    for command in commands.drain(0..){
                        let responses = match self.engine.request(command).await {
                            Ok(resp) => resp,
                            Err(err) => RESP::Error("Unexpected".to_owned(), err.to_string()),
                        };
                        debug!("Response is {:?}", responses);
                        match self.redis_cmd.write_async(responses, true).await {
                            Ok(()) => (),
                            Err(err) => {
                                error!("Error when writing to client={}", err);
                                break;
                            }
                        }    
                    }
                } else {
                    break;
                },
                Err(err) => {
                    info!("Stopping loop, received error {}", err);
                    break;
                }
            }
        }
        info!("Connection dropped {}", self);
    }
}
