//! Inline table-driven CRC32 (IEEE 802.3, reflected polynomial 0xEDB88320) avoids
//! another dependency. Each log frame carries a checksum so recovery can find the
//! exact start of a torn or corrupted tail.

const fn make_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

const CRC_TABLE: [u32; 256] = make_table();

pub fn crc32(bytes: &[u8]) -> u32 {
    let mut c = 0xFFFF_FFFF_u32;
    for &b in bytes {
        c = CRC_TABLE[((c ^ u32::from(b)) & 0xFF) as usize] ^ (c >> 8);
    }
    c ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::crc32;

    /// The standard CRC-32 check value: crc32(b"123456789") == 0xCBF43926.
    #[test]
    fn crc32_reference() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }
}
