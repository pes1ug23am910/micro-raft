//! Canonical bounded binary representation for replicated staging/retry payloads.
use serde::{Deserialize, Deserializer, Serializer};
pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    let mut text = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8] = b"0123456789abcdef";
    for byte in bytes {
        text.push(HEX[usize::from(byte >> 4)] as char);
        text.push(HEX[usize::from(byte & 15)] as char);
    }
    serializer.serialize_str(&text)
}
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    let text = String::deserialize(deserializer)?;
    if text.len() > crate::MAX_TRANSFER_BYTES * 2 || !text.len().is_multiple_of(2) {
        return Err(serde::de::Error::custom("invalid compact byte length"));
    }
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            digit(pair[0])
                .zip(digit(pair[1]))
                .map(|(a, b)| a * 16 + b)
                .ok_or_else(|| serde::de::Error::custom("invalid compact byte encoding"))
        })
        .collect()
}
