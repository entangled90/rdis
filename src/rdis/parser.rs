use std::time::Instant;
use super::protocol::RESP;
use super::types::ResultT;
use nom::*;
use nom::{
    branch::alt,
    bytes::complete::{tag, take, take_until},
    character::complete::{alphanumeric1, char, crlf, digit1, space1},
    combinator::{map, opt},
    multi::{count, separated_list1},
    sequence::{preceded, terminated, tuple},
};
use std::convert::TryInto;
use std::sync::Arc;
use log::info;

fn read_positive_decimal(bytes: &[u8]) -> IResult<&[u8], u64> {
    let (rem, int_bytes) = digit1(bytes)?;
    // FIX ERROR HANDLING
    let int: u64 = String::from_utf8(int_bytes.to_vec())
        .unwrap()
        .parse()
        .unwrap();
    Ok((rem, int))
}

fn read_decimal(bytes: &[u8]) -> IResult<&[u8], i64> {
    let (rem, (minus, int)) = tuple((opt(char('-')), read_positive_decimal))(bytes)?;
    Ok((
        rem,
        if minus.is_some() {
            -(int as i64)
        } else {
            int as i64
        },
    ))
}

// supports null
fn read_bulk(bytes: &[u8]) -> IResult<&[u8], RESP> {
    let (rem, size) = preceded(char('$'), terminated(read_decimal, crlf))(bytes)?;
    if size > 0 {
        let us: u64 = size.try_into().unwrap();
        terminated(map(take(us), |b: &[u8]| RESP::BulkString(Arc::new(b.into()))), crlf)(rem)
    } else {
        Ok((rem, RESP::Null))
    }
}

fn read_simple(bytes: &[u8]) -> IResult<&[u8], RESP> {
    let parser = preceded(char('+'), terminated(take_until("\r\n"), crlf));
    map(parser, |s : &[u8]| RESP::SimpleString(s.into()))(bytes)
}

fn read_error(bytes: &[u8]) -> IResult<&[u8], RESP> {
    map(
        preceded(
            char('-'),
            tuple((alphanumeric1, preceded(space1, take_until("\r\n")))),
        ),
        |(e, desc)| RESP::Error(read_string(e), read_string(desc)),
    )(bytes)
}

fn read_string(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn read_integer(bytes: &[u8]) -> IResult<&[u8], RESP> {
    let parser = preceded(char(':'), terminated(read_decimal, crlf));
    map(parser, |b| RESP::Integer(b))(bytes)
}

fn read_primitive(bytes: &[u8]) -> IResult<&[u8], RESP> {
    alt((read_integer, read_simple, read_bulk, read_error))(bytes)
}

fn read_array(bytes: &[u8]) -> IResult<&[u8], RESP> {
    let (rem, size) = preceded(char('*'), terminated(read_positive_decimal, crlf))(bytes)?;
    map(count(read_primitive, size as usize), |v| RESP::Array(v))(rem)
}

fn read_inline_commands(bytes: &[u8]) -> IResult<&[u8], RESP> {
    let (rem, v) = terminated(separated_list1(space1, alphanumeric1), crlf)(bytes)?;
    let mut v_simple = Vec::with_capacity(v.len());
    for b in v {
        v_simple.push(RESP::SimpleString(b.into()));
    }
    Ok((rem, RESP::Array(v_simple)))
}

pub fn read(bytes: &[u8]) -> IResult<&[u8], RESP>  {
    alt((
        read_integer,
        read_simple,
        read_bulk,
        read_error,
        read_array,
        read_inline_commands,
    ))(bytes)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    #[test]
    pub fn test_read_simple() {
        let res = read(b"+OK!! \r\n").unwrap();
        assert_eq!(res.0.len(), 0);
        assert_eq!(RESP::SimpleString("OK!! ".into()), res.1);
    }

    #[test]
    pub fn test_read_bulk_easy() {
        let res = read(b"$5\r\nhello\r\n").unwrap();
        assert_eq!(res.0.len(), 0);
        assert_eq!(RESP::BulkString(Arc::new("hello".into())), res.1);
    }

    #[test]
    pub fn test_read_decimal_easy() {
        assert_eq!(RESP::Integer(299), read(b":299\r\n").unwrap().1);
    }
    #[test]
    pub fn test_read_decimal_negative() {
        assert_eq!(RESP::Integer(-299), read(b":-299\r\n").unwrap().1);
    }

    #[test]
    pub fn test_read_decimal_should_fail() {
        match read(b"c299") {
            Ok(_) => panic!("test failed"),
            Err(_) => (),
        }
        match read(b"") {
            Ok(_) => panic!("test failed"),
            Err(_) => (),
        }
    }

    #[test]
    pub fn test_read_decimal_rem() {
        assert_eq!(RESP::Integer(299), read(b":299\r\nbdc").unwrap().1);
        assert_eq!(b"bdc", read(b":299\r\nbdc").unwrap().0);
    }

    #[test]
    pub fn test_read_null() {
        assert_eq!(RESP::Null, read(b"$-1\r\n").unwrap().1);
    }

    #[test]
    pub fn test_read_array() {
        assert_eq!(
            RESP::Array(vec![
                RESP::BulkString(Arc::new("hello".into())),
                RESP::BulkString(Arc::new("world".into()))
            ]),
            read_array(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
                .unwrap()
                .1
        );
        assert_eq!(RESP::Array(vec![]), read_array(b"*0\r\n").unwrap().1);
    }
}
