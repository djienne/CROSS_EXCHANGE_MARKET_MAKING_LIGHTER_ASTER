//! Dry-run mode: the unchanged live bot trades against an in-process simulation of Aster and
//! Lighter as a bot in AWS Tokyo would see them.
//!
//! The simulated world is the real one delayed by a constant shift D: real feed events are
//! replayed D late, so this host's own feed lag (Europe) is hidden and the latency a Tokyo bot
//! would pay is modelled explicitly. `matching` is the venues' deterministic core; `book` holds
//! the replica each market trades against; `account` settles the money; `clock` draws the
//! latencies.

pub mod account;
pub mod book;
pub mod clock;
pub mod matching;
