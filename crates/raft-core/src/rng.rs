//! Inline PCG32 (PCG-XSH-RR 32/64) — the ONLY randomness in `raft-core`.
//!
//! Implemented from the algorithm description in M.E. O'Neill's PCG paper
//! (pcg-random.org); no external crates. The generator's
//! identity is pinned forever by `pcg32_reference_sequence`: elections draw
//! their timeouts from this generator, so a silently-changed RNG would silently
//! change every seeded simulation result in the project.

/// The 64-bit LCG multiplier from the PCG reference implementation.
const MULTIPLIER: u64 = 6364136223846793005;

/// Stream (increment) selector. 54 is the stream used by the published
/// `pcg32-demo`, so `Pcg32::new(42)` reproduces the official reference vector.
const DEFAULT_STREAM: u64 = 54;

/// PCG-XSH-RR 32/64: 64-bit LCG state, 32-bit output via xorshift + rotate.
#[derive(Clone, Debug)]
pub struct Pcg32 {
    state: u64,
    inc: u64,
}

impl Pcg32 {
    /// Seeded exactly like the reference `pcg32_srandom_r(seed, DEFAULT_STREAM)`.
    pub fn new(seed: u64) -> Self {
        let mut rng = Pcg32 {
            state: 0,
            inc: (DEFAULT_STREAM << 1) | 1,
        };
        rng.next_u32();
        rng.state = rng.state.wrapping_add(seed);
        rng.next_u32();
        rng
    }

    pub fn next_u32(&mut self) -> u32 {
        let old = self.state;
        self.state = old.wrapping_mul(MULTIPLIER).wrapping_add(self.inc);
        let xorshifted = (((old >> 18) ^ old) >> 27) as u32;
        let rot = (old >> 59) as u32;
        xorshifted.rotate_right(rot)
    }

    /// Draw from `lo..=hi` by modulo. The modulo bias is ~span/2^32 — for the
    /// 151-value election-timeout band it is irrelevant; determinism, not
    /// perfect uniformity, is the property this project depends on.
    pub fn range_inclusive(&mut self, lo: u64, hi: u64) -> u64 {
        debug_assert!(lo <= hi, "range_inclusive: lo must be <= hi");
        let span = hi - lo + 1;
        lo + u64::from(self.next_u32()) % span
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the generator's identity forever: seed 42
    /// must reproduce the published pcg32-demo reference sequence for
    /// `pcg32_srandom(42, 54)`, independently reproduced with a second
    /// implementation before being hardcoded here. If this test ever
    /// fails, the RNG changed — and every seeded simulation result in the
    /// project silently changed with it.
    #[test]
    fn pcg32_reference_sequence() {
        let mut rng = Pcg32::new(42);
        let got: Vec<u32> = (0..8).map(|_| rng.next_u32()).collect();
        assert_eq!(
            got,
            vec![
                0xa15c02b7, 0x7b47f409, 0xba1d3330, 0x83d2f293, 0xbfa4784b, 0xcbed606e, 0xbfc6a3ad,
                0x812fff6d,
            ]
        );
    }

    /// 10k draws from the election-timeout band: all within bounds, both
    /// endpoints observed (the deadline math in R6/R8 depends on inclusivity).
    #[test]
    fn pcg32_range_inclusive_bounds() {
        let mut rng = Pcg32::new(0x00C0_FFEE);
        let mut saw_lo = false;
        let mut saw_hi = false;
        for _ in 0..10_000 {
            let v = rng.range_inclusive(150, 300);
            assert!((150..=300).contains(&v), "draw {v} out of [150, 300]");
            saw_lo |= v == 150;
            saw_hi |= v == 300;
        }
        assert!(saw_lo, "lower endpoint 150 never drawn in 10k draws");
        assert!(saw_hi, "upper endpoint 300 never drawn in 10k draws");
    }
}
