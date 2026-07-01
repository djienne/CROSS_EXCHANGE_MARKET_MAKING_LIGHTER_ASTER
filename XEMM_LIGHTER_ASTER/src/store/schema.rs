//! SQLite schema. Decimals are stored as TEXT, timestamps as ISO-8601 (RFC3339)
//! TEXT, PKs as UUID TEXT. Booleans as INTEGER 0/1. All `CREATE`s are idempotent.

pub const PRAGMAS: &str = "
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=OFF;
";

pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS runs (
    run_id       TEXT PRIMARY KEY,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    mode         TEXT NOT NULL,
    events_path  TEXT,
    code_version TEXT,
    config_json  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS markets (
    run_id             TEXT NOT NULL,
    market             TEXT NOT NULL,
    aster_symbol       TEXT NOT NULL,
    hl_coin            TEXT NOT NULL,
    tick_size          TEXT NOT NULL,
    step_size          TEXT NOT NULL,
    aster_min_qty      TEXT NOT NULL,
    aster_min_notional TEXT NOT NULL,
    hl_sz_decimals     INTEGER NOT NULL,
    hl_qty_step        TEXT NOT NULL,
    hl_min_notional    TEXT NOT NULL,
    PRIMARY KEY (run_id, market)
);

-- The accepted (place/requote) opportunity stream is stored as aggregates, not
-- per-row. The evaluator (re)posts a quote on every book move; at 14 markets x 2
-- sides x 3 queue models that is millions of rows per run, and the report only ever
-- reads them back as SUM/COUNT/AVG. So the engine folds them into in-memory counters
-- (store/db.rs OppAgg) and writes one summary row per (market, side, queue_model) at
-- run end. `sum_*` columns are accumulated in event order, so `sum/accepted`
-- reproduces the old `AVG(CAST(... AS REAL))` to display precision. The full
-- per-quote detail stays deterministically reconstructable by replaying the tape.
CREATE TABLE IF NOT EXISTS opportunity_stats (
    run_id               TEXT NOT NULL,
    market               TEXT NOT NULL,
    side                 TEXT NOT NULL,
    queue_model          TEXT NOT NULL,
    accepted             INTEGER NOT NULL,
    sum_instant_edge_bps REAL NOT NULL,   -- over accepted only; mean = sum/accepted
    sum_distance_bps     REAL NOT NULL,   -- over accepted only; mean = sum/accepted
    size_clamped         INTEGER NOT NULL,
    queue_truncated      INTEGER NOT NULL, -- accepted quotes resting beyond captured depth20
    PRIMARY KEY (run_id, market, side, queue_model)
);

-- Rejects, by contrast, are kept per-row with a timestamp: they are sparse (logged
-- only when the reject reason changes, store/sim/engine.rs) and individually
-- interesting -- e.g. ASTER_POSITION_CAP_REACHED rows mark exactly when/where a side
-- stopped quoting because the position hit its cap (one-sided-quoting analysis).
CREATE TABLE IF NOT EXISTS opportunity_rejects (
    run_id        TEXT NOT NULL,
    market        TEXT NOT NULL,
    side          TEXT NOT NULL,
    queue_model   TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    event_ts      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_rej_run ON opportunity_rejects(run_id, market, queue_model);

-- Quote revisions follow the same firehose-to-aggregate rule as accepted
-- opportunities above: the evaluator requotes on book moves, so per-row storage
-- grew without bound (a live week produced ~3.6M rows = ~97% of a 1.5 GB db)
-- while nothing ever read the rows back. The engine folds them into in-memory
-- counters (store/db.rs) and writes one summary row per (market, side,
-- queue_model, reason) at run end; per-revision detail stays reconstructable by
-- replaying the tape. The legacy `quote_revisions` per-row table is dropped by
-- the startup maintenance in db.rs.
CREATE TABLE IF NOT EXISTS quote_revision_stats (
    run_id      TEXT NOT NULL,
    market      TEXT NOT NULL,
    side        TEXT NOT NULL,
    queue_model TEXT NOT NULL,
    reason      TEXT NOT NULL,
    revisions   INTEGER NOT NULL,
    PRIMARY KEY (run_id, market, side, queue_model, reason)
);

CREATE TABLE IF NOT EXISTS simulated_fills (
    id                             TEXT PRIMARY KEY,
    run_id                         TEXT NOT NULL,
    quote_id                       TEXT NOT NULL,
    market                         TEXT NOT NULL,
    queue_model                    TEXT NOT NULL,
    aster_side                     TEXT NOT NULL,
    fill_px                        TEXT NOT NULL,
    fill_qty                       TEXT NOT NULL,
    sweep_print_px                 TEXT NOT NULL,
    -- The resting quote's quoted spread at the moment it filled (the "spread used"
    -- for this trade); pair with hedges.realized_edge_bps for quoted-vs-realized.
    quoted_edge_bps                TEXT NOT NULL,
    quoted_distance_bps            TEXT NOT NULL,
    remaining_quote_qty_after_fill TEXT NOT NULL,
    was_trade_through              INTEGER NOT NULL,
    was_partial                    INTEGER NOT NULL,
    -- The matched feed was stale when this fill landed: a resting quote that could
    -- not be cancelled in time was still hit during cancel latency (a stale-window
    -- adverse fill). Pair with hedges.hedged_on_stale_book.
    feed_stale_at_fill             INTEGER NOT NULL,
    -- The quote rested beyond Aster's captured depth20, so the queue ahead could not
    -- be fully observed (the seeded queue is a lower bound; the fill may be optimistic).
    queue_truncated                INTEGER NOT NULL,
    aster_pos_notional             TEXT,
    hl_pos_notional                TEXT,
    exch_ts                        TEXT NOT NULL,
    local_recv_ts                  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fill_run ON simulated_fills(run_id, market, queue_model);

CREATE TABLE IF NOT EXISTS hedges (
    id                   TEXT PRIMARY KEY,
    run_id               TEXT NOT NULL,
    fill_id              TEXT,
    market               TEXT NOT NULL,
    queue_model          TEXT NOT NULL,
    hedge_side           TEXT NOT NULL,
    qty                  TEXT NOT NULL,   -- requested/dispatched hedge size
    filled_qty           TEXT NOT NULL,   -- size that actually filled vs the HL book
    aster_fill_px        TEXT NOT NULL,
    hl_vwap              TEXT NOT NULL,
    latency_bucket_ms    INTEGER NOT NULL,
    gross_pnl            TEXT NOT NULL,
    aster_fee            TEXT NOT NULL,
    hl_fee               TEXT NOT NULL,
    net_pnl              TEXT NOT NULL,
    realized_edge_bps    TEXT NOT NULL,
    hl_slippage_bps      TEXT,
    depth_exhausted      INTEGER NOT NULL,
    hedged_on_stale_book INTEGER NOT NULL,
    fill_local_ts        TEXT NOT NULL,
    resolve_ts           TEXT NOT NULL,
    hl_book_ts           TEXT NOT NULL,
    -- Non-NULL only for an exceptional resolution, e.g. MISSING_HL_BOOK (no HL book
    -- existed at resolve time, so the hedge could not price — filled_qty = 0).
    reason               TEXT
);
CREATE INDEX IF NOT EXISTS ix_hedge_run ON hedges(run_id, market, queue_model, latency_bucket_ms);

CREATE TABLE IF NOT EXISTS pending_inventory_events (
    id               TEXT PRIMARY KEY,
    run_id           TEXT NOT NULL,
    market           TEXT NOT NULL,
    queue_model      TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    signed_qty       TEXT NOT NULL,
    avg_aster_px     TEXT NOT NULL,
    mark_px          TEXT,
    pending_notional TEXT NOT NULL,
    realized_pnl     TEXT,
    first_fill_ts    TEXT,
    last_fill_ts     TEXT,
    event_ts         TEXT NOT NULL,
    reason           TEXT
);
CREATE INDEX IF NOT EXISTS ix_pending_run ON pending_inventory_events(run_id, market, queue_model);
"#;
