use super::parser;
use super::types::*;
use async_recursion::async_recursion;
use bytes::{Buf, BytesMut};
use log::{debug, error, info, warn};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::prelude::AsyncRead;
use tokio::prelude::AsyncWrite;

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum RESP {
    SimpleString(Vec<u8>),
    Error(String, String),
    Integer(i64),
    BulkString(Arc<Vec<u8>>),
    Array(Vec<RESP>),
    Null,
}

impl RESP {
    pub async fn write_end<W>(b: &mut W) -> ResultT<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        for c in CRLF.iter() {
            b.write_u8(*c).await?;
        }
        Ok(())
    }

    #[async_recursion]
    pub async fn write_async<W>(self, writer: &mut W, flush: bool) -> ResultT<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        match self {
            RESP::SimpleString(s) => {
                writer.write_u8(b'+').await?;
                writer.write_all(&s.as_slice()).await?;
                RESP::write_end(writer).await?;
            }
            RESP::Error(err_type, err) => {
                writer.write_u8(b'-').await?;
                writer.write_all(&err_type.as_bytes()).await?;
                writer.write_all(&err.as_bytes()).await?;
                RESP::write_end(writer).await?;
            }
            RESP::Integer(int) => {
                let string_rep: String = int.to_string();
                writer.write_u8(b':').await?;
                writer.write_all(&string_rep.as_bytes()).await?;
                RESP::write_end(writer).await?;
            }
            RESP::BulkString(s) => {
                let len = s.len().to_string();
                writer.write_u8(b'$').await?;
                writer.write_all(&len.as_bytes()).await?;
                RESP::write_end(writer).await?;
                writer.write_all(&s).await?;
                RESP::write_end(writer).await?;
            }
            RESP::Array(mut vec) => {
                writer.write_u8(b'*').await?;
                writer.write_all(&vec.len().to_string().as_bytes()).await?;
                RESP::write_end(writer).await?;
                for el in vec.drain(0..vec.len()) {
                    el.write_async(writer, false).await?;
                }
            }
            RESP::Null => writer.write_all(NULL_MSG).await?,
        };
        if flush {
            writer.flush().await?;
        }
        Ok(())
    }
}

const CRLF: [u8; 2] = [b'\r', b'\n'];
const NULL_MSG: &[u8] = b"$-1\r\n";

pub struct RedisCmd<R, W> {
    // pub stream: TcpStream,
    writer: W,
    reader: R,
    buff: BytesMut,
    client_epoch: usize,
    pipelined_request: Vec<RESP>,
}

impl RedisCmd<OwnedReadHalf, BufWriter<OwnedWriteHalf>> {
    pub fn from_stream(
        stream: TcpStream,
        client_epoch: usize,
    ) -> RedisCmd<OwnedReadHalf, BufWriter<OwnedWriteHalf>> {
        let (reader, writer) = stream.into_split();
        RedisCmd::new(reader, BufWriter::new(writer), client_epoch)
    }
}

impl<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send + Debug> RedisCmd<R, W> {
    pub fn new(r: R, w: W, client_epoch: usize) -> RedisCmd<R, W> {
        RedisCmd {
            writer: w,
            reader: r,
            buff: BytesMut::with_capacity(4096),
            client_epoch,
            pipelined_request: Vec::with_capacity(1024),
        }
    }
    // requests are read all togethere, in order to minimize write operations as well
    pub async fn read_async(&mut self) -> ResultT<ClientReq> {
        loop {
            match self.parse_frame() {
                Ok(resp) => {
                    if let Some(r) = resp {
                        self.pipelined_request.push(r);
                    }
                }
                Err(err) => {
                    if self.pipelined_request.len() > 0 {
                        // info!("returning req #{}", self.pipelined_request.len());
                        return Ok(self.fill_output_pipeline_req());
                    } else {
                        if self.buff.capacity() == 0 {
                            self.buff.reserve(2 * self.buff.len());
                            warn!("Expanding buffer to {}", self.buff.len());
                        }
                        let n = self.reader.read_buf(&mut self.buff).await?;
                        debug!(
                            "Read {} bytes from socket from client {}",
                            n, self.client_epoch
                        );
                        if n == 0 {
                            // The remote closed the connection. For this to be
                            // a clean shutdown, there should be no data in the
                            // read buffer. If there is, this means that the
                            // peer closed the socket while sending a frame.
                            return Ok(self.fill_output_pipeline_req());
                        }
                    }
                }
            }
        }
    }

    fn fill_output_pipeline_req(&mut self) -> ClientReq {
        let received = self.pipelined_request.len();
        if received == 1 {
            ClientReq::Single(self.pipelined_request.pop().unwrap())
        } else {
            let mut sendable = Vec::with_capacity(received);
            {
                for r in self.pipelined_request.drain(0..) {
                    sendable.push(r);
                }
            }
            ClientReq::Pipeline(sendable)
        }
    }

    pub async fn write_async(&mut self, resp: RESP, flush: bool) -> ResultT<()> {
        resp.write_async(&mut self.writer, flush).await
    }

    fn parse_frame(&mut self) -> ResultT<Option<RESP>> {
        let slice = &self.buff;
        let size = slice.len();
        let (rem, resp) = match parser::read(slice) {
            Ok((rem, resp)) => Ok((Some(rem), Some(resp))),
            Err(nom::Err::Incomplete(_)) => Ok((None, None)),
            Err(err) => Err(ErrorT::from(format!("Fatal parsing error {}", err))),
        }?;
        let rem_size = rem.map_or(0, |r| r.len());
        self.buff = self.buff.split_off(size - rem_size);
        Ok(resp)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClientReq {
    Single(RESP),
    Pipeline(Vec<RESP>),
}

use ClientReq::*;

impl Into<Vec<RESP>> for ClientReq {
    fn into(self) -> Vec<RESP> {
        match self {
            Single(r) => vec![r],
            Pipeline(v) => v,
        }
    }
}

impl ClientReq {
    pub fn len(&self) -> usize {
        match self {
            Single(_) => 1,
            Pipeline(rs) => rs.len(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::super::types::*;
    use super::RedisCmd;
    use super::RESP;
    use std::io::Cursor;
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    pub async fn test_resp_encoding() -> ResultT<()> {
        let mut req: Vec<(RESP, Vec<u8>)> = vec![
            (RESP::SimpleString("OK".into()), b"+OK\r\n".to_vec()),
            (RESP::Integer(129), b":129\r\n".to_vec()),
            (
                RESP::BulkString(Arc::new("foobar".into())),
                b"$6\r\nfoobar\r\n".to_vec(),
            ),
            (RESP::Null, b"$-1\r\n".to_vec()),
            (
                RESP::Array(vec![
                    RESP::BulkString(Arc::new("foo".into())),
                    RESP::BulkString(Arc::new("bar".into())),
                ]),
                b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".to_vec(),
            ),
            (
                RESP::Array(vec![1, 2, 3].iter().map(|i| RESP::Integer(*i)).collect()),
                b"*3\r\n:1\r\n:2\r\n:3\r\n".to_vec(),
            ),
            (RESP::Null, b"$-1\r\n".to_vec()),
        ];
        for (en, bytes) in req.drain(0..req.len()) {
            let mut b = Cursor::new(Vec::new());
            en.write_async(&mut b, true).await?;
            assert_eq!(b.into_inner(), bytes);
        }
        Ok(())
    }

    #[tokio::test]
    pub async fn test_pipeline_req() -> ResultT<()> {
        let (client, server) = tokio::io::duplex(64);
        let mut cmd = RedisCmd::new(client, server, 1);
        let sent_msg = RESP::SimpleString("PING".into());
        for i in 0..3i8 {
            cmd.write_async(sent_msg.clone(), i == 2).await?;
        }
        let mut resp: Vec<_> = cmd.read_async().await?.into();
        assert_eq!(resp.len(), 3);
        for r in resp.drain(0..) {
            assert_eq!(r, sent_msg)
        }
        Ok(())
    }

    #[tokio::test]
    pub async fn test_pipeline_req_benchmark() -> ResultT<()> {
        let (client, server) = tokio::io::duplex(1024);
        let mut cmd = RedisCmd::new(client, server, 0);
        let pipeline_reqs = b"PING\r\nPING\r\nPING\r\n";
        cmd.writer.write_all(pipeline_reqs).await?;
        let mut resp: Vec<_> = cmd.read_async().await?.into();
        // it's an array because it uses the compact form
        let sent_msg = RESP::Array(vec![RESP::SimpleString("PING".into())]);
        assert_eq!(resp.len(), 3);
        for r in resp.drain(0..) {
            assert_eq!(r, sent_msg)
        }
        Ok(())
    }
}
