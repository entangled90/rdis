use super::types::*;
use bytes::{Bytes, BytesMut, BufMut, Buf};
use core::fmt::Write;
use std::ops::Deref;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::TcpStream;
use super::parser;

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum RESP {
    SimpleString(String),
    Error(String, String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RESP>),
    Null,
}

impl Into<ResultT<Bytes>> for RESP {
    fn into(self) -> ResultT<Bytes> {
        let mut b = BytesMut::with_capacity(3);
        self.write(&mut b)?;
        Ok(b.freeze())
    }
}

impl RESP {
    pub fn write_end<W>(b: &mut W) -> ResultT<()>
    where
        W: Write,
    {
        for c in CRLF.iter() {
            b.write_char(*c)?;
        }
        Ok(())
    }

    pub fn into_bytes(self) -> ResultT<Bytes> {
        let mut b = BytesMut::with_capacity(self.size_hint());
        self.write(&mut b)?;
        Ok(b.freeze())
    }

    pub fn size_hint(&self) -> usize {
        match self {
            RESP::SimpleString(s) => 1 + s.len() + CRLF.len(),
            RESP::Error(err_type, err) => 1 + err_type.len() + 1 + err.len() + CRLF.len(),
            RESP::Integer(int) => 1 + int.to_string().len() + CRLF.len(),
            RESP::BulkString(s) => 1 + s.len().to_string().len() + s.len() + CRLF.len(),
            RESP::Array(vec) => vec.iter().fold(3, |prev, el| prev + el.size_hint()),
            RESP::Null => NULL_MSG.len(),
        }
    }

    pub fn write<W>(self, writer: &mut W) -> ResultT<()>
    where
        W: Write,
    {
        match self {
            RESP::SimpleString(s) => {
                writer.write_char('+')?;
                writer.write_str(&s)?;
                RESP::write_end(writer)?;
            }
            RESP::Error(err_type, err) => {
                writer.write_char('-')?;
                writer.write_str(&err_type)?;
                writer.write_str(&err)?;
                RESP::write_end(writer)?;
            }
            RESP::Integer(int) => {
                let string_rep: String = int.to_string();
                writer.write_char(':')?;
                writer.write_str(&string_rep)?;
                RESP::write_end(writer)?;
            }
            RESP::BulkString(s) => {
                let len = s.len().to_string();
                writer.write_char('$')?;
                writer.write_str(&len)?;
                RESP::write_end(writer)?;
                writer.write_str(&s)?;
                RESP::write_end(writer)?;
            }
            RESP::Array(mut vec) => {
                writer.write_char('*')?;
                writer.write_str(&vec.len().to_string())?;
                RESP::write_end(writer)?;
                for el in vec.drain(0..vec.len()) {
                    el.write(writer)?;
                }
            }
            RESP::Null => writer.write_str(NULL_MSG)?,
        };
        Ok(())
    }

    pub async fn write_async<W>(self, w: &mut W) -> ResultT<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        //TODO 1 extra copy to avoid dup
        w.write_all(self.into_bytes()?.deref()).await?;
        Ok(())
    }
}

const CRLF: [char; 2] = ['\r', '\n'];
const NULL_MSG: &str = "$-1\r\n";

#[cfg(test)]
mod tests {
    use super::super::types::*;
    use super::RESP;
    #[test]
    pub fn test_resp_encoding() -> ResultT<()> {
        let mut req: Vec<(RESP, Vec<u8>)> = vec![
            (RESP::SimpleString("OK".to_owned()), b"+OK\r\n".to_vec()),
            (RESP::Integer(129), b":129\r\n".to_vec()),
            (
                RESP::BulkString("foobar".to_owned()),
                b"$6\r\nfoobar\r\n".to_vec(),
            ),
            (RESP::Null, b"$-1\r\n".to_vec()),
            (
                RESP::Array(vec![
                    RESP::BulkString("foo".to_owned()),
                    RESP::BulkString("bar".to_owned()),
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
            let b = en.into_bytes()?;
            assert_eq!(*b, *bytes.as_slice());
        }
        Ok(())
    }
}

pub struct RedisCmd {
    pub stream: TcpStream,
    buff: BytesMut
}

impl RedisCmd  {
    pub fn new(stream: TcpStream) -> RedisCmd {
        RedisCmd {
            stream,
            buff: BytesMut::with_capacity(4096),
            // cursor: 0,
            // cmd: None
        }
    }

    pub async fn read_async(& mut self) -> ResultT<Option<RESP>>{
        loop{
            match self.parse_frame() {
                Ok(resp) => 
                    return Ok(resp),
                Err(_) =>{
                    if !self.buff.has_remaining(){
                        self.buff.reserve(self.buff.len());
                    }
                    let n = self.stream.read_buf(&mut self.buff).await?;
                    if n == 0 {
                         // The remote closed the connection. For this to be
                        // a clean shutdown, there should be no data in the
                        // read buffer. If there is, this means that the
                        // peer closed the socket while sending a frame.
                        return Ok(None)
                    } else {
                        return Err("Connection reset by peer".into())
                    }
                }
            }
        }
    }

    fn parse_frame(& mut self) -> ResultT<Option<RESP>>{
        let (rem , resp) = parser::read(&self.buff)?;
        self.buff = BytesMut::from(rem);
        // self.buff.clear();
        // self.buff.put(rem);
        Ok(Some(resp))
    }
}
