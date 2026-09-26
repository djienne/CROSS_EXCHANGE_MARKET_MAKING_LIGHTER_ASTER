# Pair screener

Which pairs listed on both Aster and Lighter would suit the bot's two strategies? The screener
answers from public data. It needs no credentials, sends no orders, and runs apart from the bot,
with its own crate, image, container and data.

<!-- ft-facts: container=aster-lighter-screener image=aster_lighter_screener:latest -->

- **`collect`** (the container) follows every pair's best bid/offer and trades on both venues. It
  records only the moments the report could trade on, plus 5-minute summaries per pair:
  - **taker:** an edge reaching the bot's entry gate (the 90th percentile of its samples over 72 h),
    whose samples the summaries count;
  - **XEMM:** a trade that could have filled a quote at the lowest edge scored (5 bps), with the
    second before it (what the quote was priced from);
  - each moment keeps the second after it (where latency-delayed orders fill).
- **`report`** replays the bot's rules on those moments and ranks the pairs.
  - **Taker-taker:** the entry gate, cooldown, inventory cap and each leg's latency.
  - **XEMM:** the quote price, a fill only when a trade prints through the quote, the hedge latency
    and the distance gate.

  Fees, latencies and thresholds apply at report time (`screener.toml` `[report]`). What is recorded
  follows from `[report]` at its loosest, so the same data answers Standard vs Premium, a latency up
  to ×2.9, a higher margin, gate percentile or XEMM edge. The report refuses settings that would
  trade on moments the data did not record (a lower percentile, another depth or gate window). A
  test checks that the recording keeps every trade the report would make on the full stream.

The pairs are the perps on both venues whose names match and whose prices agree within 2%, with
at least $200k 24 h volume on each venue (64 on 2026-09-26). They are refreshed at each UTC
midnight.

## Run

```bash
docker compose up -d --build                        # collect (start_all.bat starts it too)
docker compose logs -f                              # a traffic line every 5 min
docker compose run --rm screener universe           # today's pairs, with their Aster taker fee
docker compose run --rm report                      # the ranking, from all the data
docker compose run --rm report --since 2026-09-27 --lighter premium --json
docker compose run --rm report --latency 2          # sensitivity: every latency doubled
```

`report` is its own service (profile `report`, never started by `up`): it holds every recorded
state of the days it scores in memory, ~0.5 GB a day, under an 8 GB cap.

## Reading the report

- **Units.** $/day at one $13 clip, over the days both venues were followed.
- **`tk`** is the bot's gated taker, which needs 50 samples of opportunities to leave its warmup.
- **`kept`** is realized/expected edge after latency.
- **`xb`** is XEMM as the bot runs it: Aster maker, Lighter hedge, required edge 18.5 bps, 18–50 bps
  behind the Aster touch.
- **`xm`** / **`xr`** are the best required edge (`req`) of a sweep without the distance gate, for
  Aster maker → Lighter hedge and for the reverse.
- **`+days`** counts the days on which the best strategy made money.
- **Rank correlation.** The last line gives the day-to-day rank correlation. Near 0 means the
  ranking is noise, and needs more days before anyone acts on it.

## Known limits

- **Top of book only.** Top of book stands in for the bot's VWAP over 10× the clip; moments with
  less than that at the top are skipped. For a pair worth trading, record its full depth
  (`LIGHTER_ASTER_BOT`, `record --market <X>`) and replay precisely.
- **The gate.** The gate's samples are counted in 0.25 bps bins, from the pair's required edge at
  the cheaper Lighter tier, and it reads whole 5-minute windows: it opens within a bin of the bot's,
  a window later. At the Premium tier its samples are the Standard ones above Premium's edge.
- **Arrival times.** Times are arrival times on this host (Europe), not Tokyo exchange time. Both
  venues are in Tokyo, so the offsets mostly cancel.
- **XEMM fills.** XEMM ignores queue position (a fill needs a trade *through* the quote) and our
  own market impact.
- **No funding.** Funding is not scored: Lighter's funding sources disagree on units. Check it by
  hand for a candidate pair.

## Files

`data/<YYYY-MM-DD>T<HHMMSS>Z.screen.zst` holds one file per run (a run ends at each UTC midnight).
Each file is zstd-compressed tab-separated lines, with the formats in `src/collect.rs`. A kill
loses at most the last 30 s. A new run reads the last 72 h of summaries back for its gate.
