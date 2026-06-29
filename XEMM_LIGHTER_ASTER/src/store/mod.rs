//! SQLite persistence layer: schema definitions and the typed `Db` writer.

pub mod db;
pub mod schema;

pub use db::{
    Db, FillRow, HedgeRow, OpportunityRow, PendingEventRow, QuoteRevisionRow,
};
