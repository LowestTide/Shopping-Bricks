# Walkthrough of pipeline/pipeline.py (read this tomorrow)

Status: passes all 36 fixtures and matches dev-answer exactly. Not yet tuned for speed.

## Study order (each stage = one SQL idea, in the order the code runs)
1. **Parse** (`raw_lines`, `unq`, `parse_ts`): read each line as text, check it with a regex, split on commas.
   SQL ideas: `string_split`, `regexp_full_match`, `CASE WHEN`, macros (`CREATE MACRO`).
2. **Dedup** (`ev_dedup`): `GROUP BY event_id` + `arg_min(..., ingest_ts)` = "keep the row with the smallest ingest_ts".
3. **Validate**: a `WHERE` with all the rules. Order matters: dedup first, validate second.
4. **Links**: union-find / connected components, done with scipy because SQL is clumsy for long chains.
5. **Sessions**: `LAG` gives the gap to the previous event; a running `MAX(CASE WHEN gap > 30min THEN ts END) OVER (...)`
   stamps every event with its session start.
6. **Attribution**: `ASOF JOIN` = "for each session, the latest click at or before it".
7. **Assists**: merge each person+campaign's clicks into 7-day "islands", then join sessions to islands.
8. **Outputs**: `GROUP BY` person/day and campaign/day, `FULL OUTER JOIN` credited with assisted.

## Run locally (no Docker)
    pip install duckdb==1.5.6 numpy scipy pyarrow polars
    SCRATCH_DIR=/tmp/scr python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py

## Known shortcuts to be aware of
* Inputs under 64 MB use the exact (slow) text reader so `\r` is treated as an ordinary character (fixture E20).
  Bigger inputs use DuckDB's fast line scanner, which treats `\r` as a line break; the generator never writes `\r`.
