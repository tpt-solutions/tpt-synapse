//! RESP (REdis Serialization Protocol) wire codec (spec.txt §6 Phase 2).
//!
//! [`decode_value`] is the untrusted-input entry point: it parses one RESP
//! value from the front of `&[u8]`, returning `Ok(None)` when more bytes are
//! needed, or `Err` on malformed input. Commands arrive as RESP arrays of bulk
//! strings, which the broker extracts via [`Value::as_array`].

use std::fmt;

/// A decoded RESP value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    NullBulk,
    Array(Vec<Value>),
    NullArray,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespError {
    Incomplete,
    Malformed(&'static str),
}

impl fmt::Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespError::Incomplete => f.write_str("incomplete RESP value"),
            RespError::Malformed(r) => write!(f, "malformed RESP: {r}"),
        }
    }
}

impl std::error::Error for RespError {}

impl Value {
    /// If this value is an array, return its elements; otherwise `None`.
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(v) => Some(v),
            _ => None,
        }
    }

    /// Interpret a bulk string / simple string element as an ASCII string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::BulkString(b) => std::str::from_utf8(b).ok(),
            Value::SimpleString(s) => Some(s),
            _ => None,
        }
    }
}

fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn read_line(buf: &[u8], start: usize) -> Result<Option<(usize, usize)>, RespError> {
    match find_crlf(buf, start) {
        Some(crlf) => Ok(Some((crlf, crlf + 2))),
        None => {
            // A line may still be in flight; only treat as incomplete if we
            // haven't yet seen a full CRLF and there is no CRLF terminator.
            if start < buf.len() {
                Ok(None)
            } else {
                Err(RespError::Incomplete)
            }
        }
    }
}

/// Decode one RESP value from the front of `buf`.
pub fn decode_value(buf: &[u8]) -> Result<Option<(Value, usize)>, RespError> {
    if buf.is_empty() {
        return Ok(None);
    }
    let marker = buf[0];
    match marker {
        b'+' => {
            let (end, next) = match read_line(buf, 1)? {
                Some(x) => x,
                None => return Ok(None),
            };
            let s = std::str::from_utf8(&buf[1..end])
                .map_err(|_| RespError::Malformed("bad utf8"))?
                .to_string();
            Ok(Some((Value::SimpleString(s), next)))
        }
        b'-' => {
            let (end, next) = match read_line(buf, 1)? {
                Some(x) => x,
                None => return Ok(None),
            };
            let s = std::str::from_utf8(&buf[1..end])
                .map_err(|_| RespError::Malformed("bad utf8"))?
                .to_string();
            Ok(Some((Value::Error(s), next)))
        }
        b':' => {
            let (end, next) = match read_line(buf, 1)? {
                Some(x) => x,
                None => return Ok(None),
            };
            let s = std::str::from_utf8(&buf[1..end])
                .map_err(|_| RespError::Malformed("bad utf8"))?;
            let n = s
                .parse::<i64>()
                .map_err(|_| RespError::Malformed("bad integer"))?;
            Ok(Some((Value::Integer(n), next)))
        }
        b'$' => {
            let (end, next) = match read_line(buf, 1)? {
                Some(x) => x,
                None => return Ok(None),
            };
            let len: i64 = std::str::from_utf8(&buf[1..end])
                .map_err(|_| RespError::Malformed("bad utf8"))?
                .parse()
                .map_err(|_| RespError::Malformed("bad bulk length"))?;
            if len < 0 {
                return Ok(Some((Value::NullBulk, next)));
            }
            let len = len as usize;
            let data_start = next;
            let data_end = data_start + len;
            let crlf_end = data_end + 2;
            if buf.len() < crlf_end {
                return Ok(None);
            }
            if buf[data_end] != b'\r' || buf[data_end + 1] != b'\n' {
                return Err(RespError::Malformed("bulk string not CRLF terminated"));
            }
            Ok(Some((
                Value::BulkString(buf[data_start..data_end].to_vec()),
                crlf_end,
            )))
        }
        b'*' => {
            let (end, next) = match read_line(buf, 1)? {
                Some(x) => x,
                None => return Ok(None),
            };
            let count: i64 = std::str::from_utf8(&buf[1..end])
                .map_err(|_| RespError::Malformed("bad utf8"))?
                .parse()
                .map_err(|_| RespError::Malformed("bad array count"))?;
            if count < 0 {
                return Ok(Some((Value::NullArray, next)));
            }
            let count = count as usize;
            let mut items = Vec::with_capacity(count);
            let mut pos = next;
            for _ in 0..count {
                match decode_value(&buf[pos..])? {
                    Some((v, consumed)) => {
                        items.push(v);
                        pos += consumed;
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((Value::Array(items), pos)))
        }
        _ => Err(RespError::Malformed("unknown RESP marker")),
    }
}

/// Convenience entry point used by the fuzz targets: decode one value from
/// untrusted bytes. Never panics on malformed input.
pub fn parse(input: &[u8]) -> Result<Option<(Value, usize)>, RespError> {
    decode_value(input)
}

/// Encode `value` into `out` (RESP wire format).
pub fn encode_value(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::SimpleString(s) => {
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Error(s) => {
            out.push(b'-');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Integer(n) => {
            out.push(b':');
            out.extend_from_slice(n.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::BulkString(b) => {
            out.push(b'$');
            out.extend_from_slice(b.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
        }
        Value::NullBulk => out.extend_from_slice(b"$-1\r\n"),
        Value::Array(items) => {
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_value(item, out);
            }
        }
        Value::NullArray => out.extend_from_slice(b"*-1\r\n"),
    }
}

/// Helper builders for common responses.
impl Value {
    pub fn ok() -> Value {
        Value::SimpleString("OK".to_string())
    }
    pub fn null() -> Value {
        Value::NullBulk
    }
    pub fn bulk(s: impl Into<Vec<u8>>) -> Value {
        Value::BulkString(s.into())
    }
    pub fn int(n: i64) -> Value {
        Value::Integer(n)
    }
    pub fn err(s: impl Into<String>) -> Value {
        Value::Error(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: Value) {
        let mut buf = Vec::new();
        encode_value(&v, &mut buf);
        let (decoded, n) = decode_value(&buf).unwrap().unwrap();
        assert_eq!(decoded, v);
        assert_eq!(n, buf.len());
    }

    #[test]
    fn simple_types_roundtrip() {
        roundtrip(Value::ok());
        roundtrip(Value::int(123));
        roundtrip(Value::bulk(b"hello".to_vec()));
        roundtrip(Value::null());
        roundtrip(Value::err("ERR bad"));
        roundtrip(Value::Array(vec![
            Value::bulk(b"SET".to_vec()),
            Value::bulk(b"k".to_vec()),
            Value::bulk(b"v".to_vec()),
        ]));
    }

    #[test]
    fn incomplete_returns_none() {
        let mut buf = Vec::new();
        encode_value(&Value::bulk(b"hello".to_vec()), &mut buf);
        let partial = &buf[..buf.len() - 1];
        assert_eq!(decode_value(partial).unwrap(), None);
    }

    #[test]
    fn nested_array() {
        roundtrip(Value::Array(vec![
            Value::Array(vec![Value::bulk(b"a".to_vec())]),
            Value::int(-1),
        ]));
    }
}
