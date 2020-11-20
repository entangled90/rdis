use super::protocol::RESP;
use nom::*;
use nom::{
    branch::alt,
    bytes::complete::{ take, take_until},
    character::complete::{alphanumeric1, char, crlf, digit1, space1},
    combinator::{map, opt},
    multi::count,
    sequence::{preceded, terminated, tuple},
};
use std::convert::TryInto;

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
        terminated(map(take(us), |b| RESP::BulkString(read_string(b))), crlf)(rem)
    } else {
        Ok((rem, RESP::Null))
    }
}

fn read_simple(bytes: &[u8]) -> IResult<&[u8], RESP> {
    let parser = preceded(char('+'), terminated(alphanumeric1, crlf));
    map(parser, |s| RESP::SimpleString(read_string(s)))(bytes)
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


pub fn read<'a>(bytes: &'a[u8]) -> IResult<&'a[u8], RESP> {
    alt((read_integer, read_simple, read_bulk, read_error, read_array))(bytes)
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_read_simple() {
        let res = read(b"+OK\r\n").unwrap();
        assert_eq!(res.0.len(), 0);
        assert_eq!(RESP::SimpleString("OK".to_owned()), res.1);
    }

    #[test]
    pub fn test_read_bulk_easy() {
        let res = read(b"$5\r\nhello\r\n").unwrap();
        assert_eq!(res.0.len(), 0);
        assert_eq!(RESP::BulkString("hello".to_owned()), res.1);
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
                RESP::BulkString("hello".to_owned()),
                RESP::BulkString("world".to_owned())
            ]),
            read_array(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n").unwrap().1
        );
        assert_eq!(
            RESP::Array(vec![]),
            read_array(b"*0\r\n").unwrap().1
        );
    }
}
