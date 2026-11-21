# How the Rust pipeline works (src/main.rs)

Status: passes all 36 fixtures and matches dev-answer exactly. The earlier DuckDB version is kept in
`legacy/` (also in git history) as a fallback; it scored 34.82 s. The Rust version is roughly 12x faster
on the same machine in my tests (20M events: 93 s -> 7.9 s on 2 cores).

## Why it is faster than SQL
* **No generic machinery.** SQL engines parse text with regexes, compare strings and write temporary tables.
  Here each line is parsed once, straight from the file bytes, with a few byte comparisons.
* **Numbers instead of strings.** User IDs and event IDs become 64/128-bit hashes right away, so sorting,
  grouping and joining compare integers.
* **All 8 cores, no waiting.** Files are cut into 8 MB chunks that threads (rayon) parse independently.
* **Memory-mapped input.** Files are mapped, not copied; the OS pages them in as needed.

## Stages (search for these letters in the code)
* A. Parse: split lines, check quote rules, convert timestamps with plain arithmetic, validate.
* C. Links: union-find (a "who is connected to whom" structure) -> one key per person, named by the smallest ID.
* B. Dedup: shuffle events into 512 buckets by event-ID hash; sort each bucket; keep the earliest `ingest_ts`.
* D. Per person: sort by time, cut sessions at gaps over 30 min, credit the latest click, and count assists
  by merging each campaign's clicks into 7-day "islands".
* E. Output: both tables as Parquet, written in parallel.

## Run locally
    cargo build --release
    DATA_DIR=... OUTPUT_DIR=... NCPU=8 target/release/pipeline
    python3 scripts/run_fixtures.py fixtures -- target/release/pipeline
