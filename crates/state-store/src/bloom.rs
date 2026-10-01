use crate::invalid;
use serde::{Deserialize, Serialize};
use std::io;

pub(crate) const BITS_PER_KEY: usize = 10;
const PROBES: u32 = 7;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Bloom {
    bits: Vec<u8>,
    probes: u32,
    keys: usize,
}

fn hashes(key: &[u8]) -> (u64, u64) {
    let mut a = 0xcbf2_9ce4_8422_2325u64;
    let mut b = 0x9e37_79b9_7f4a_7c15u64;
    for &byte in key {
        a = (a ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3);
        b = (b ^ u64::from(byte))
            .rotate_left(13)
            .wrapping_mul(0xbf58_476d_1ce4_e5b9);
    }
    (a, b | 1)
}

impl Bloom {
    pub(crate) fn build(keys: &[&[u8]]) -> Self {
        let mut filter = Self {
            bits: vec![0; (keys.len() * BITS_PER_KEY).div_ceil(8).max(8)],
            probes: PROBES,
            keys: keys.len(),
        };
        for key in keys {
            let (a, b) = hashes(key);
            let length = (filter.bits.len() * 8) as u64;
            for probe in 0..PROBES {
                let bit = a.wrapping_add(u64::from(probe).wrapping_mul(b)) % length;
                filter.bits[bit as usize / 8] |= 1 << (bit % 8);
            }
        }
        filter
    }
    pub(crate) fn validate(&self, keys: usize) -> io::Result<()> {
        if self.keys != keys
            || self.probes != PROBES
            || self.bits.len() != (keys * BITS_PER_KEY).div_ceil(8).max(8)
        {
            return Err(invalid("invalid Bloom encoding"));
        }
        Ok(())
    }
    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        let (a, b) = hashes(key);
        let length = (self.bits.len() * 8) as u64;
        (0..self.probes).all(|probe| {
            let bit = a.wrapping_add(u64::from(probe).wrapping_mul(b)) % length;
            self.bits[bit as usize / 8] & (1 << (bit % 8)) != 0
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn serialized_filter_has_no_false_negatives() {
        let keys: Vec<_> = (0u32..10_000).map(|n| n.to_le_bytes()).collect();
        let refs: Vec<_> = keys.iter().map(|key| key.as_slice()).collect();
        let filter = Bloom::build(&refs);
        let reopened: Bloom =
            serde_json::from_slice(&serde_json::to_vec(&filter).unwrap()).unwrap();
        reopened.validate(keys.len()).unwrap();
        assert!(keys.iter().all(|key| reopened.contains(key)));
        let positives = (10_000u32..20_000)
            .filter(|n| reopened.contains(&n.to_le_bytes()))
            .count();
        assert!(
            positives < 1000,
            "unexpected filter failure: {positives}/10000"
        );
    }
}
