#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB pipeline.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)

How to read this file (each numbered stage is one SQL idea):
  0. Setup            - DuckDB settings, the window from manifest.json
  1. Raw lines        - read every file as *lines of text* (we parse them ourselves,
                        because DuckDB's CSV parser does not follow the rules' dialect)
  2. Parse + clean    - split a line into fields, unquote, validate, convert timestamps
  3. Links            - connected components (union-find via scipy) -> one name per person
  4. Sessions         - window functions: LAG finds gaps, a running MAX numbers sessions
  5. Attribution      - ASOF JOIN: "latest click at or before the session start"
  6. Assists          - which campaigns had a click in the 7 days before a session
  7. Output           - GROUP BY the two daily tables
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import duckdb
import numpy as np
import pyarrow as pa
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:7.2f}s] {msg}", file=sys.stderr, flush=True)


# --------------------------------------------------------------------------- 0. setup
con = duckdb.connect()  # in-memory database
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")  # leave headroom for Python
os.makedirs(os.path.join(SCRATCH, "duckdb_tmp"), exist_ok=True)
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")  # we never rely on row order

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)


def iso_to_ms(s):
    return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()) * 1000


WS = iso_to_ms(manifest["window_start"])  # inclusive
WE = iso_to_ms(manifest["window_end"])  # exclusive
DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
GAP_MS = 30 * 60 * 1000
log(f"window {manifest['window_start']} .. {manifest['window_end']}")

# --------------------------------------------------------------------------- 1. raw lines
# DuckDB's CSV reader always treats '\r' as a line break, but the rules say '\r' is an
# ordinary character. read_text + string_split is exact but ~100x slower, so it is only used
# on tiny inputs (the fixtures, which are the only data containing '\r'; the generator
# never writes one). Everything bigger goes through the fast line scanner.
EXACT_BYTES_LIMIT = 64 * 1024 * 1024


def dir_bytes(sub):
    d = os.path.join(DATA, sub)
    return sum(e.stat().st_size for e in os.scandir(d) if e.name.endswith(".csv"))


EXACT = sum(dir_bytes(s) for s in ("events", "clicks", "links")) <= EXACT_BYTES_LIMIT
log(f"line reader: {'exact (read_text)' if EXACT else 'fast (read_csv line scanner)'}")


def raw_lines(sub):
    """SQL for a one-column table `line`: every data line (header skipped) of every file."""
    pattern = os.path.join(DATA, sub, "*.csv")
    if EXACT:
        return (f"SELECT unnest(string_split(content, chr(10))[2:]) AS line "
                f"FROM read_text('{pattern}')")
    return (f"SELECT line FROM read_csv('{pattern}', columns={{'line': 'VARCHAR'}}, auto_detect=false, "
            f"delim='\\x01', quote='', escape='', header=false, skip=1, new_line='\\n', strict_mode=false) "
            f"WHERE line IS NOT NULL")


# --------------------------------------------------------------------------- 2. parse helpers
# A line is well formed when it has exactly N fields and every field is either free of
# quotes or wrapped in one pair of quotes. (No value ever contains a comma or a quote.)
def line_regex(n):
    field = '(?:"[^",]*"|[^",]*)'
    return field + "(?:," + field + ")" + "{" + str(n - 1) + "}"


# unquote a field; an empty field (quoted or not) is NULL
con.execute("""CREATE MACRO unq(x) AS
    NULLIF(CASE WHEN starts_with(x, '"') THEN substr(x, 2, length(x) - 2) ELSE x END, '')""")

# timestamp text -> epoch milliseconds, or NULL when it is a bad timestamp.
#   F1  2026-10-03T23:58:10Z          F2  2026-10-03T19:58:10-04:00          F3  1791071890000
# Regexes use [0-9] (ASCII digits only). try_strptime rejects impossible calendar values.
con.execute("""CREATE MACRO parse_ts(s) AS
    CASE
      WHEN s IS NULL THEN NULL
      WHEN regexp_full_match(s, '[0-9]+') THEN
           (CASE WHEN length(ltrim(s, '0')) <= 18 THEN TRY_CAST(s AS BIGINT) END)
      WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z') THEN
           epoch_ms(try_strptime(substr(s, 1, 19), '%Y-%m-%dT%H:%M:%S'))
      WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-]([01][0-9]|2[0-3]):[0-5][0-9]') THEN
           epoch_ms(try_strptime(substr(s, 1, 19), '%Y-%m-%dT%H:%M:%S'))
           - (CASE WHEN s[20] = '-' THEN -1 ELSE 1 END)
             * (CAST(substr(s, 21, 2) AS BIGINT) * 60 + CAST(substr(s, 24, 2) AS BIGINT)) * 60000
    END""")

# --------------------------------------------------------------------------- 2a. events
t = time.time()
con.execute(f"""
CREATE TEMP TABLE ev_raw AS
WITH l AS ({raw_lines('events')}),
ok AS (SELECT string_split(line, ',') AS p FROM l WHERE regexp_full_match(line, '{line_regex(7)}'))
SELECT unq(p[1]) AS event_id, unq(p[2]) AS user_id, unq(p[3]) AS ts, unq(p[4]) AS ingest_ts,
       unq(p[5]) AS event_type, unq(p[6]) AS product_id, unq(p[7]) AS price
FROM ok
WHERE unq(p[1]) IS NOT NULL
""")
log(f"events parsed ({time.time() - t:.1f}s)")

# Dedup on event_id: keep the copy with the smallest ingest_ts (ingest_ts is fixed-width F1,
# so comparing it as text is the same as comparing instants). Dedup is BEFORE validation.
con.execute("""
CREATE TEMP TABLE ev_dedup AS
SELECT event_id, arg_min(struct_pack(user_id := user_id, ts := ts, event_type := event_type, product_id := product_id,
                        price := price, ingest_ts := ingest_ts), ingest_ts) AS r
FROM ev_raw GROUP BY event_id
""")
con.execute("DROP TABLE ev_raw")
log("events deduped")

# Validate the kept copy, convert timestamps, drop late events.
con.execute(f"""
CREATE TEMP TABLE ev_valid AS
WITH x AS (
  SELECT r.user_id AS user_id, parse_ts(r.ts) AS ts, parse_ts(r.ingest_ts) AS ing,
         r.event_type AS event_type, r.product_id AS product_id,
         CASE WHEN r.event_type = 'purchase' AND regexp_full_match(r.price, '[0-9]+')
                   AND length(ltrim(r.price, '0')) <= 8 THEN TRY_CAST(r.price AS BIGINT) END AS price
  FROM ev_dedup
)
SELECT user_id, ts,
       CASE WHEN event_type = 'purchase' THEN price END AS price,
       CASE WHEN event_type = 'purchase' THEN product_id END AS product_id
FROM x
WHERE user_id IS NOT NULL
  AND ts >= {WS} AND ts < {WE}
  AND event_type IN ('view', 'cart', 'purchase')
  AND product_id IS NOT NULL
  AND (event_type <> 'purchase' OR price BETWEEN 1 AND 10000000)
  AND ing - ts <= {DAY_MS}
""")
con.execute("DROP TABLE ev_dedup")
log("events validated: %d rows" % con.execute("SELECT count(*) FROM ev_valid").fetchone()[0])

# --------------------------------------------------------------------------- 2b. clicks
con.execute("""CREATE TEMP TABLE campaigns AS
    SELECT campaign_id, channel FROM read_csv('%s', header=true, all_varchar=true,
           columns={'campaign_id': 'VARCHAR', 'channel': 'VARCHAR'})""" % os.path.join(DATA, "campaigns.csv"))
con.execute("""CREATE TEMP TABLE products AS
    SELECT product_id, category FROM read_csv('%s', header=true, all_varchar=true,
           columns={'product_id': 'VARCHAR', 'category': 'VARCHAR'})""" % os.path.join(DATA, "products.csv"))

con.execute(f"""
CREATE TEMP TABLE clicks_valid AS
WITH l AS ({raw_lines('clicks')}),
ok AS (SELECT string_split(line, ',') AS p FROM l WHERE regexp_full_match(line, '{line_regex(4)}')),
c AS (SELECT unq(p[1]) AS click_id, unq(p[2]) AS user_id, unq(p[3]) AS campaign_id, parse_ts(unq(p[4])) AS ts FROM ok)
SELECT DISTINCT click_id, user_id, campaign_id, ts      -- copies are identical: DISTINCT is the dedup
FROM c
WHERE click_id IS NOT NULL AND user_id IS NOT NULL
  AND ts >= {WS - WEEK_MS} AND ts < {WE}
  AND campaign_id IN (SELECT campaign_id FROM campaigns)
""")
log("clicks validated: %d rows" % con.execute("SELECT count(*) FROM clicks_valid").fetchone()[0])

# --------------------------------------------------------------------------- 3. links
# Each valid link line joins two IDs. Joining is transitive => connected components.
# SQL is awkward for chains hundreds of links long, so: number the IDs in SQL, run
# scipy's connected_components (C code, ~1 s for millions of edges), and bring the result back.
con.execute(f"""
CREATE TEMP TABLE links AS
WITH l AS ({raw_lines('links')}),
ok AS (SELECT string_split(line, ',') AS p FROM l WHERE regexp_full_match(line, '{line_regex(2)}'))
SELECT DISTINCT unq(p[1]) AS a, unq(p[2]) AS b FROM ok
WHERE unq(p[1]) IS NOT NULL AND unq(p[2]) IS NOT NULL AND unq(p[1]) <> unq(p[2])
""")
n_links = con.execute("SELECT count(*) FROM links").fetchone()[0]
if n_links:
    con.execute("""
    CREATE TEMP TABLE lid AS
    SELECT id, (row_number() OVER ()) - 1 AS idx FROM (SELECT a AS id FROM links UNION SELECT b FROM links)
    """)
    n_ids = con.execute("SELECT count(*) FROM lid").fetchone()[0]
    edges = con.execute("""
        SELECT la.idx AS i, lb.idx AS j FROM links k JOIN lid la ON la.id = k.a JOIN lid lb ON lb.id = k.b
    """).fetchnumpy()
    graph = coo_matrix((np.ones(len(edges["i"]), dtype=np.int8), (edges["i"], edges["j"])), shape=(n_ids, n_ids))
    _, labels = connected_components(graph, directed=False)
    con.register("lab_arrow", pa.table({"idx": np.arange(n_ids, dtype=np.int64), "label": labels.astype(np.int64)}))
    # name = byte-wise smallest ID of the component (DuckDB compares VARCHAR bytewise)
    con.execute("""
    CREATE TEMP TABLE person AS
    SELECT id, name FROM (
        SELECT l.id AS id, min(l.id) OVER (PARTITION BY b.label) AS name
        FROM lid l JOIN lab_arrow b USING (idx)
    ) WHERE id <> name
    """)
    con.unregister("lab_arrow")
else:
    con.execute("CREATE TEMP TABLE person (id VARCHAR, name VARCHAR)")
log(f"links resolved: {n_links} links")

# --------------------------------------------------------------------------- 4. sessions
# Replace each ID by its person's name (IDs that are not in `person` are their own name).
con.execute("""
CREATE TEMP TABLE ev AS
SELECT coalesce(p.name, e.user_id) AS person, e.ts, e.price, e.product_id
FROM ev_valid e LEFT JOIN person p ON p.id = e.user_id
""")
con.execute("DROP TABLE ev_valid")

# Step 1: gap to the previous event of the same person (LAG, sorted by ts).
# Step 2: a session starts where the gap is NULL (first event) or > 30 min; the running MAX
#         of those start timestamps is the session start of every event. The default window
#         frame treats equal timestamps as peers, so events sharing a ts share a session.
con.execute(f"""
CREATE TEMP TABLE ev_s AS
WITH g AS (
  SELECT person, ts, price, product_id,
         ts - lag(ts) OVER (PARTITION BY person ORDER BY ts) AS gap
  FROM ev
)
SELECT person, price, product_id,
       max(CASE WHEN gap IS NULL OR gap > {GAP_MS} THEN ts END) OVER (PARTITION BY person ORDER BY ts) AS sstart
FROM g
""")
con.execute("DROP TABLE ev")
con.execute("""
CREATE TEMP TABLE sess AS
SELECT person, sstart, count(*) AS events, coalesce(sum(price), 0) AS revenue, count(price) AS purchases
FROM ev_s GROUP BY person, sstart
""")
log("sessions: %d" % con.execute("SELECT count(*) FROM sess").fetchone()[0])

# --------------------------------------------------------------------------- 5. attribution
# Clicks become the person's clicks; for equal (person, ts) keep the smallest click_id.
con.execute("""
CREATE TEMP TABLE pclicks AS
SELECT coalesce(p.name, c.user_id) AS person, c.ts, c.campaign_id
FROM clicks_valid c LEFT JOIN person p ON p.id = c.user_id
""")
con.execute("""
CREATE TEMP TABLE pclicks1 AS
SELECT coalesce(p.name, c.user_id) AS person, c.ts, min(c.click_id) AS click_id,
       arg_min(c.campaign_id, c.click_id) AS campaign_id
FROM clicks_valid c LEFT JOIN person p ON p.id = c.user_id
GROUP BY 1, 2
""")
# ASOF JOIN: for each session, the click with the greatest ts that is <= session start.
# If that latest click is older than 7 days, no click qualifies => 'organic'.
con.execute(f"""
CREATE TEMP TABLE sess_cr AS
SELECT s.person, s.sstart, s.events, s.revenue, s.purchases,
       CASE WHEN c.ts IS NOT NULL AND s.sstart - c.ts <= {WEEK_MS} THEN c.campaign_id ELSE 'organic' END AS campaign_id
FROM sess s ASOF LEFT JOIN pclicks1 c ON s.person = c.person AND s.sstart >= c.ts
""")
log("attribution done")

# --------------------------------------------------------------------------- 6. assists
# A campaign assists a session if one of the person's clicks of that campaign lies in
# [start - 7d, start]. Per (person, campaign) the clicks are merged into "islands"
# (a click within 7d of the previous one extends the island), so each session meets
# each campaign at most once.
con.execute(f"""
CREATE TEMP TABLE islands AS
WITH x AS (
  SELECT person, campaign_id, ts,
         ts - lag(ts) OVER (PARTITION BY person, campaign_id ORDER BY ts) AS gap
  FROM (SELECT DISTINCT person, campaign_id, ts FROM pclicks)
), y AS (
  SELECT person, campaign_id, ts,
         max(CASE WHEN gap IS NULL OR gap > {WEEK_MS} THEN ts END)
             OVER (PARTITION BY person, campaign_id ORDER BY ts) AS island_start
  FROM x
)
SELECT person, campaign_id, island_start AS lo, max(ts) + {WEEK_MS} AS hi
FROM y GROUP BY person, campaign_id, island_start
""")
con.execute("""
CREATE TEMP TABLE assists AS
SELECT i.campaign_id, CAST(epoch_ms(s.sstart) AS DATE) AS day, count(*) AS assisted
FROM sess s JOIN islands i ON i.person = s.person AND s.sstart >= i.lo AND s.sstart <= i.hi
GROUP BY 1, 2
""")
log("assists done")

# --------------------------------------------------------------------------- 7. outputs
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)

# user_daily: one row per (person, day); top_category from purchases only.
con.execute("""
CREATE TEMP TABLE topcat AS
SELECT person, day, category AS top_category FROM (
  SELECT person, day, category, total,
         row_number() OVER (PARTITION BY person, day ORDER BY total DESC, category ASC) AS rn
  FROM (
    SELECT e.person, CAST(epoch_ms(e.sstart) AS DATE) AS day,
           coalesce(p.category, 'unknown') AS category, sum(e.price) AS total
    FROM ev_s e LEFT JOIN products p ON p.product_id = e.product_id
    WHERE e.price IS NOT NULL
    GROUP BY 1, 2, 3
  )
) WHERE rn = 1
""")
con.execute(f"""
COPY (
  SELECT s.person AS user_id, s.day, s.sessions::BIGINT AS sessions, s.events::BIGINT AS events,
         s.revenue_cents::BIGINT AS revenue_cents, t.top_category
  FROM (SELECT person, CAST(epoch_ms(sstart) AS DATE) AS day, count(*) AS sessions,
               sum(events) AS events, sum(revenue) AS revenue_cents
        FROM sess GROUP BY 1, 2) s
  LEFT JOIN topcat t ON t.person = s.person AND t.day = s.day
) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")
log("user_daily written")

con.execute(f"""
COPY (
  WITH cr AS (
    SELECT campaign_id, CAST(epoch_ms(sstart) AS DATE) AS day, count(*) AS sessions,
           count(*) FILTER (WHERE purchases > 0) AS purchasing_sessions, sum(revenue) AS revenue_cents
    FROM sess_cr GROUP BY 1, 2
  )
  SELECT coalesce(cr.campaign_id, a.campaign_id) AS campaign_id,
         coalesce(c.channel, 'organic') AS channel,
         coalesce(cr.day, a.day) AS day,
         coalesce(cr.sessions, 0)::BIGINT AS sessions,
         coalesce(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
         coalesce(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
         coalesce(a.assisted, 0)::BIGINT AS assisted_sessions
  FROM cr FULL OUTER JOIN assists a ON cr.campaign_id = a.campaign_id AND cr.day = a.day
  LEFT JOIN campaigns c ON c.campaign_id = coalesce(cr.campaign_id, a.campaign_id)
) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")
log("campaign_daily written; done")
