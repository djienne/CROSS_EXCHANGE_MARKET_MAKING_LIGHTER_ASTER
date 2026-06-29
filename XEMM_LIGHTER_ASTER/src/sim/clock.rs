//! Virtual-clock helpers for the replay engine: locate the HL book to hedge
//! against at a target time, and bound the per-market HL book ring.

use std::collections::VecDeque;

use chrono::{DateTime, Utc};

use crate::book::OrderBook;

/// The HL book *in effect* at `target`: the most recent snapshot whose receive
/// time is at-or-before `target` (a market order sent at `target` executes
/// against the current book, not a future one — avoiding lookahead bias). Falls
/// back to the earliest book if `target` precedes the ring (caller flags
/// staleness from the age gap).
pub fn last_hl_book_at_or_before(
    ring: &VecDeque<OrderBook>,
    target: DateTime<Utc>,
) -> Option<&OrderBook> {
    ring.iter()
        .rev()
        .find(|b| b.local_recv_ts <= target)
        .or_else(|| ring.front())
}

/// Drop ring entries older than `window_ms` before `now`, always keeping at
/// least the most recent book.
pub fn prune_ring(ring: &mut VecDeque<OrderBook>, now: DateTime<Utc>, window_ms: i64) {
    while ring.len() > 1 {
        match ring.front() {
            Some(front) if (now - front.local_recv_ts).num_milliseconds() > window_ms => {
                ring.pop_front();
            }
            _ => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn book_at(secs: i64) -> OrderBook {
        let t = DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap();
        OrderBook::from_levels(vec![(dec!(100), dec!(1))], vec![(dec!(101), dec!(1))], t, t)
    }

    #[test]
    fn finds_book_in_effect() {
        let mut ring = VecDeque::new();
        ring.push_back(book_at(0));
        ring.push_back(book_at(2));
        ring.push_back(book_at(4));
        // target between the t=2 and t=4 snapshots => the t=2 book is in effect.
        let target = DateTime::from_timestamp(1_700_000_003, 0).unwrap();
        let b = last_hl_book_at_or_before(&ring, target).unwrap();
        assert_eq!(b.local_recv_ts, book_at(2).local_recv_ts);
    }

    #[test]
    fn fallback_to_earliest_when_target_precedes_ring() {
        let mut ring = VecDeque::new();
        ring.push_back(book_at(5));
        ring.push_back(book_at(7));
        let target = DateTime::from_timestamp(1_700_000_001, 0).unwrap();
        let b = last_hl_book_at_or_before(&ring, target).unwrap();
        assert_eq!(b.local_recv_ts, book_at(5).local_recv_ts);
    }

    #[test]
    fn prune_keeps_window() {
        let mut ring = VecDeque::new();
        for s in 0..10 {
            ring.push_back(book_at(s));
        }
        let now = DateTime::from_timestamp(1_700_000_009, 0).unwrap();
        prune_ring(&mut ring, now, 3_000); // keep last 3s
        assert!(ring.len() <= 5);
        assert!(ring.front().unwrap().local_recv_ts >= book_at(6).local_recv_ts);
    }
}
