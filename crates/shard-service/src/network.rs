//! Bounded HTTP/1 exchange for the trusted cluster control plane.
use serde::{de::DeserializeOwned, Serialize};
use std::{io, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
pub const MAX_BODY: usize = 512 * 1024;
// A checked response can contain both bounded committed and effective histories.
pub const MAX_RESPONSE: usize = 8 * 1024 * 1024;
const MAX_HEADER: usize = 16 * 1024;
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub async fn post<T: Serialize, R: DeserializeOwned>(
    addr: SocketAddr,
    path: &str,
    body: &T,
    deadline: Duration,
) -> io::Result<R> {
    if !matches!(path, "/v1/query" | "/v1/execute") {
        return Err(invalid("unexpected internal request path"));
    }
    let bytes = crate::canonical_bytes(body, MAX_BODY)
        .map_err(|_| invalid("outbound request too large"))?;
    tokio::time::timeout(deadline,async {
        let mut stream=TcpStream::connect(addr).await?;stream.set_nodelay(true)?;
        let header=format!("POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",bytes.len());
        stream.write_all(header.as_bytes()).await?;stream.write_all(&bytes).await?;
        let mut response=Vec::new();let mut buf=[0u8;16*1024];
        loop {
            let n=stream.read(&mut buf).await?;
            if n==0{break;}
            if response.len()+n>MAX_HEADER+MAX_RESPONSE{return Err(invalid("response exceeds bound"));}
            response.extend_from_slice(&buf[..n]);
            if !response.windows(4).any(|p|p==b"\r\n\r\n") && response.len()>MAX_HEADER{return Err(invalid("header exceeds bound"));}
        }
        decode(&response)
    }).await.map_err(|_|io::Error::new(io::ErrorKind::TimedOut,"cluster request deadline"))?
}
fn decode<R: DeserializeOwned>(bytes: &[u8]) -> io::Result<R> {
    let end = bytes
        .windows(4)
        .position(|p| p == b"\r\n\r\n")
        .ok_or_else(|| invalid("incomplete response header"))?;
    if end > MAX_HEADER {
        return Err(invalid("response header too large"));
    }
    let header =
        std::str::from_utf8(&bytes[..end]).map_err(|_| invalid("non-ASCII response header"))?;
    let mut lines = header.split("\r\n");
    if !matches!(lines.next(), Some("HTTP/1.1 200 OK" | "HTTP/1.0 200 OK")) {
        return Err(invalid("unexpected HTTP response status"));
    }
    let mut length = None;
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| invalid("malformed response header"))?;
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(invalid("ambiguous/chunked response framing unsupported"));
        }
        if name.eq_ignore_ascii_case("content-length") {
            let value = value.trim();
            if length.is_some() || value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid("ambiguous content length"));
            }
            length = Some(
                value
                    .parse::<usize>()
                    .map_err(|_| invalid("invalid content length"))?,
            );
        }
    }
    let body = &bytes[end + 4..];
    if length != Some(body.len()) || body.len() > MAX_RESPONSE {
        return Err(invalid("incomplete/oversize response body"));
    }
    serde_json::from_slice(body).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checked_membership_with_numeric_endpoint_keys_round_trips() {
        use crate::{query::Observed, runtime::Response, service::Wire};
        use raft_core::membership::{CommittedMembership, MemberEndpoints};
        let mut committed =
            CommittedMembership::bootstrap_with_group("wire-group".into(), vec![1, 2, 3]).unwrap();
        committed.state.learners.insert(4);
        committed.state.endpoints.insert(
            4,
            MemberEndpoints {
                raft: "127.0.0.1:7104".into(),
                http: "127.0.0.1:8104".into(),
            },
        );
        let observed = Observed::Membership {
            group: 1,
            effective: Box::new(committed.state.clone()),
            committed: Box::new(committed),
            record: None,
        };
        let wire = Wire {
            cluster: "wire-cluster".into(),
            body: Response::Checked {
                group: 1,
                node: 2,
                term: 2,
                index: 13,
                applied_index: 13,
                context: 1,
                observed: Box::new(observed.clone()),
            },
        };
        let body = serde_json::to_vec(&wire).unwrap();
        let mut response =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
        response.extend(body);
        let decoded: Wire<Response> = decode(&response).expect("typed remote membership response");
        let Response::Checked {
            observed: actual, ..
        } = decoded.body
        else {
            panic!("wrong response variant")
        };
        assert_eq!(*actual, observed);
        for membership in ["committed", "effective"] {
            let mut malformed = serde_json::to_value(&wire).unwrap();
            let state = if membership == "committed" {
                &mut malformed["body"]["observed"][membership]["state"]
            } else {
                &mut malformed["body"]["observed"][membership]
            };
            let endpoint = state["endpoints"]
                .as_object_mut()
                .unwrap()
                .remove("4")
                .unwrap();
            state["endpoints"]["256"] = endpoint;
            assert!(serde_json::from_slice::<Wire<Response>>(
                &serde_json::to_vec(&malformed).unwrap()
            )
            .is_err());
            let mut malformed = serde_json::to_value(&wire).unwrap();
            let state = if membership == "committed" {
                &mut malformed["body"]["observed"][membership]["state"]
            } else {
                &mut malformed["body"]["observed"][membership]
            };
            state["unexpected"] = serde_json::json!(true);
            assert!(serde_json::from_slice::<Wire<Response>>(
                &serde_json::to_vec(&malformed).unwrap()
            )
            .is_err());
        }
    }
    #[test]
    fn rejects_ambiguous_partial_or_extra_response_bytes() {
        for bytes in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\n{}".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n{}"
                .as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n{}".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}x".as_slice(),
        ] {
            assert!(decode::<serde_json::Value>(bytes).is_err());
        }
        assert_eq!(
            decode::<serde_json::Value>(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").unwrap(),
            serde_json::json!({})
        );
    }
}
