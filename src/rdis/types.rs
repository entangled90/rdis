use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::io::BufWriter;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::time::Instant;

use tracing::*;
use std::error::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub type ErrorT = Box<dyn Error + Sync + Send>;
pub type ResultT<A> = Result<A, ErrorT>;

use super::protocol::*;

pub struct RedisServer {
    pub listener: TcpListener,
    open_handles: Mutex<Vec<JoinHandle<()>>>,
    client_epoch: AtomicUsize,
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
            client_epoch,
        }
    }

    pub fn add_handle(&self, handle: JoinHandle<()>) -> Option<()> {
        let mut lock = self.open_handles.lock().unwrap();
        (*lock).push(handle);
        Some(())
    }
}

pub struct RedisEngineApi {
    sender: mpsc::Sender<(ClientReq, oneshot::Sender<ClientReq>)>,
}
impl RedisEngineApi {
    pub fn new(sender: mpsc::Sender<(ClientReq, oneshot::Sender<ClientReq>)>) -> RedisEngineApi {
        RedisEngineApi {
            sender,
        }
    }

    pub async fn request(&self, req: ClientReq) -> ResultT<ClientReq> {
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
    redis_cmd: RedisCmd<OwnedReadHalf, BufWriter<OwnedWriteHalf>>,
    engine: Arc<RedisEngineApi>,
    client_epoch: usize,
}

impl Display for ClientConnection {
    fn fmt(&self, f: &mut Formatter) -> std::result::Result<(), std::fmt::Error> {
        f.write_fmt(format_args!(
            "ClientConnection{{client_epoch: {} }}",
            self.client_epoch
        ))
    }
}

impl ClientConnection {
    pub async fn start_loop(mut self) {
        info!("Connection received {}", self);
        loop {
            let before_read = Instant::now();
            let cmd = self.redis_cmd.read_async().await;
            let span = span!(Level::INFO, "message received");
            let _guard = span.enter();
            let read_delta = before_read.elapsed().as_micros();
            debug!("Time for read {}, client={}", read_delta, self.client_epoch);
            match cmd {
                Ok(commands) => {
                    let len = commands.len();
                    if len > 0 {
                        let responses = match self.engine.request(commands).await {
                            Ok(resp) => resp,
                            // not really correct
                            Err(err) => ClientReq::Single(RESP::Error(
                                "Unexpected".to_owned(),
                                err.to_string(),
                            )),
                        };
                        let mut resp_vec: Vec<_> = responses.into();
                        for (idx, response) in resp_vec.drain(0..).enumerate() {
                            debug!("Response is {:?}", response);
                            match self.redis_cmd.write_async(response, idx == len - 1).await {
                                Ok(()) => (),
                                Err(err) => {
                                    error!("Error when writing to client={}", err);
                                    break;
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
                Err(err) => {
                    info!("Stopping loop, received error {}", err);
                    break;
                }
            }
        }
        info!("Connection dropped {}", self);
    }
}
