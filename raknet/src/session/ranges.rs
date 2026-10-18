//! Compact received reliable-index ranges, ordered relative to the current base.

use crate::util::u24;

/// Start index and contiguous length in the wrapping 24-bit counter.
pub(super) type Range = (u32, u32);

/// Reports whether a received index is retained in an ahead range.
pub(super) fn contains(ranges: &[Range], base: u32, index: u32) -> bool {
    let target = u24::distance(base, index);
    let pos = ranges.partition_point(|&(start, _)| u24::distance(base, start) <= target);
    pos.checked_sub(1)
        .and_then(|prev| ranges.get(prev))
        .is_some_and(|&(start, len)| u24::distance(start, index) < len as i32)
}

/// Adds an index, merging adjacent ranges and rejecting duplicates.
pub(super) fn insert(ranges: &mut Vec<Range>, base: u32, index: u32) -> bool {
    let target = u24::distance(base, index);
    let pos = ranges.partition_point(|&(start, _)| u24::distance(base, start) <= target);

    if let Some(&(start, len)) = pos.checked_sub(1).and_then(|prev| ranges.get(prev)) {
        let offset = u24::distance(start, index);
        if offset < len as i32 {
            return false;
        }
        if offset == len as i32 {
            ranges[pos - 1].1 += 1;
            let end = u24::add(start, ranges[pos - 1].1);
            if ranges.get(pos).is_some_and(|&(next, _)| next == end) {
                let (_, next_len) = ranges.remove(pos);
                ranges[pos - 1].1 += next_len;
            }
            return true;
        }
    }

    if let Some(next) = ranges.get_mut(pos)
        && next.0 == u24::add(index, 1)
    {
        next.0 = index;
        next.1 += 1;
        return true;
    }

    ranges.insert(pos, (index, 1));
    true
}

/// Consumes the first range when its start closes the current receipt gap.
pub(super) fn take_contiguous(ranges: &mut Vec<Range>, base: u32) -> u32 {
    match ranges.first() {
        Some(&(start, len)) if start == base => {
            ranges.remove(0);
            u24::add(base, len)
        }
        _ => base,
    }
}
