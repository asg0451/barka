//! Composite log offsets packed into a single `u64`.
//!
//! Layout: **40 bits** segment sequence (high) + **24 bits** intra-segment index (low).
//! This dedicates more of the space to segment ids than a 32/32 split (~1T segments,
//! up to ~16M records per segment).

/// Bits used for the index within a segment (low end of [`log_offset_compose`]).
pub const INTRA_BITS: u32 = 24;
/// Bits used for the segment sequence number (high end).
pub const SEGMENT_BITS: u32 = 64 - INTRA_BITS;

pub const INTRA_MASK: u64 = (1u64 << INTRA_BITS) - 1;
/// Maximum valid `segment_seq` (inclusive).
pub const MAX_SEGMENT_SEQ: u64 = (1u64 << SEGMENT_BITS) - 1;

#[inline]
pub fn compose(segment_seq: u64, intra: u64) -> u64 {
    debug_assert!(segment_seq <= MAX_SEGMENT_SEQ);
    debug_assert!(intra <= INTRA_MASK);
    (segment_seq << INTRA_BITS) | (intra & INTRA_MASK)
}

#[inline]
pub fn segment(offset: u64) -> u64 {
    offset >> INTRA_BITS
}

#[inline]
pub fn intra(offset: u64) -> u64 {
    offset & INTRA_MASK
}

/// Human-readable decomposition: full 64-bit hex + zero-padded segment/intra fields.
pub fn format_decomposed(offset: u64) -> String {
    let seg = segment(offset);
    let i = intra(offset);
    format!(
        "offset_dec={offset} offset_hex=0x{offset:016x} segment=0x{seg:016x} intra=0x{i:08x} \
         (packed {SEGMENT_BITS}+{INTRA_BITS} bits)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compose_round_trip() {
        let o = compose(7, 42);
        assert_eq!(segment(o), 7);
        assert_eq!(intra(o), 42);
    }

    #[test]
    fn adjacent_segments_increase() {
        let a = compose(0, INTRA_MASK);
        let b = compose(1, 0);
        assert!(b > a);
    }
}
