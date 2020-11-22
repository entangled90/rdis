use super::parser;
use super::types::*;
use async_recursion::async_recursion;
use bytes::{Buf, BytesMut};
use std::fmt::Debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::prelude::AsyncRead;
use tokio::prelude::AsyncWrite;
use log::{info, debug, warn, error};
use std::sync::Arc;

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
    pub async fn write_async<W>(self, writer: &mut W) -> ResultT<()>
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
                    el.write_async(writer).await?;
                }
            }
            RESP::Null => writer.write_all(NULL_MSG).await?,
        };
        writer.flush().await?;
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
}

impl RedisCmd<BufReader<OwnedReadHalf>, BufWriter<OwnedWriteHalf>> {
    pub fn from_stream(
        stream: TcpStream,
    ) -> RedisCmd<BufReader<OwnedReadHalf>, BufWriter<OwnedWriteHalf>> {
        let (reader, writer) = stream.into_split();
        RedisCmd::new(BufReader::new(reader), BufWriter::new(writer))
    }
}

impl<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send + Debug> RedisCmd<R, W> {
    pub fn new(r: R, w: W) -> RedisCmd<R, W> {
        RedisCmd {
            writer: w,
            reader: r,
            buff: BytesMut::with_capacity(4096 * 8),
        }
    }

    pub async fn read_async(&mut self) -> ResultT<Option<RESP>> {
        loop {
            match self.parse_frame() {
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    if !self.buff.is_empty() {
                        debug!("Failed to parse frame because of {}", err);
                        debug!("Buffer contains {}", String::from_utf8(self.buff.to_vec()).unwrap());
                    }
                    if !self.buff.has_remaining() {
                        // double the buffer
                        self.buff.reserve(self.buff.len());
                    }
                    let n = self.reader.read_buf(&mut self.buff).await?;
                    // println!(
                    //     "Read {} bytes from socket: {:?}",
                    //     n,
                    //     String::from_utf8(self.buff.to_vec()).unwrap()
                    // );
                    if n == 0 {
                        // The remote closed the connection. For this to be
                        // a clean shutdown, there should be no data in the
                        // read buffer. If there is, this means that the
                        // peer closed the socket while sending a frame.
                        return Ok(None);
                    }
                }
            }
        }
    }

    pub async fn write_async(&mut self, resp: RESP) -> ResultT<()> {
        resp.write_async(&mut self.writer).await
    }

    fn parse_frame(&mut self) -> ResultT<Option<RESP>> {
        let (rem, resp) = parser::read(&self.buff)?;
        self.buff = BytesMut::from(rem);
        Ok(Some(resp))
    }
}

#[cfg(test)]
mod tests {

    use super::super::types::*;
    use super::RedisCmd;
    use super::RESP;
    use std::io::Cursor;
    use tokio::io::AsyncWriteExt;
    use std::sync::Arc;

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
            en.write_async(&mut b).await?;
            assert_eq!(b.into_inner(), bytes);
        }
        Ok(())
    }

    #[tokio::test]
    pub async fn test_pipeline_req() -> ResultT<()> {
        let (client, server) = tokio::io::duplex(64);
        let mut cmd = RedisCmd::new(client, server);
        let sent_msg = RESP::SimpleString("PING".into());
        for _ in 0..3i8 {
            cmd.write_async(sent_msg.clone()).await?;
        }
        for i in 0..3i8 {
            match cmd.read_async().await? {
                Some(msg) => assert_eq!(msg, sent_msg.clone()),
                None => panic!(format!("No message! at i={}", i)),
            }
        }
        Ok(())
    }



    #[tokio::test]
    pub async fn test_pipeline_req_benchmark() -> ResultT<()> {
        let (client, server) = tokio::io::duplex(1024);
        let mut cmd = RedisCmd::new(client, server);
        let pipeline_reqs  = b"PING\r\nPING\r\nPING\r\n"; 
        cmd.writer.write_all(pipeline_reqs).await?;
        for i in 0..3i8 {
            match cmd.read_async().await? {
                Some(msg) => assert_eq!(RESP::Array(vec![RESP::SimpleString("PING".into())]), msg),
                None => panic!(format!("No message! at i={}", i)),
            }
        }
        Ok(())


    }
}
