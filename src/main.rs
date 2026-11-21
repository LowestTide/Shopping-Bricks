//! Shopping Sessions pipeline in Rust.
//!
//! Stages (all parallel with rayon):
//!   A. parse      - mmap every CSV, split into ~8 MB chunks, parse lines straight from bytes
//!   B. dedup      - shuffle events into buckets by 128-bit event_id hash, keep earliest ingest_ts
//!   C. persons    - links -> union-find; each event/click gets its person's key
//!   D. sessions   - shuffle by person, sort by ts, cut sessions, attribute, count assists
//!   E. output     - two Parquet tables, written in parallel
use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const NBITS: u32 = 9;
const NB: usize = 1 << NBITS;
const CHUNK: usize = 8 << 20;
const NO_CAT: u16 = u16::MAX;

// ------------------------------------------------------------------ small utilities
/// Keys are already hashes, so the map's hasher just passes them through.
#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, b: &[u8]) {
        for &x in b {
            self.0 = self.0.wrapping_mul(31).wrapping_add(x as u64);
        }
    }
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}
type IdMap<V> = HashMap<u64, V, BuildHasherDefault<IdHasher>>;

fn bucket(h: u64) -> usize {
    (h >> (64 - NBITS)) as usize
}

/// A reference to bytes inside one of the mmapped files: file(16) | offset(32) | len(16).
fn pack(fid: u64, off: usize, len: usize) -> u64 {
    debug_assert!(off < (1 << 32) && len < (1 << 16));
    (fid << 48) | ((off as u64) << 16) | len as u64
}
fn bytes_of<'a>(maps: &'a [Mmap], r: u64) -> &'a [u8] {
    let fid = (r >> 48) as usize;
    let off = ((r >> 16) & 0xFFFF_FFFF) as usize;
    let len = (r & 0xFFFF) as usize;
    &maps[fid][off..off + len]
}

fn log(t0: &Instant, msg: &str) {
    eprintln!("[{:7.2}s] {}", t0.elapsed().as_secs_f64(), msg);
}

// ------------------------------------------------------------------ CSV dialect
/// Split a line into exactly N fields. A field is either free of quotes or one whole quoted
/// value. Returns (offset, len) of every field's content (quotes removed), or None if the
/// line is malformed.
#[inline]
fn parse_fields<const N: usize>(line: &[u8]) -> Option<[(u32, u32); N]> {
    let mut out = [(0u32, 0u32); N];
    let mut start = 0usize;
    let mut idx = 0usize;
    for p in memchr_iter(b',', line) {
        if idx >= N - 1 {
            return None;
        }
        out[idx] = unq(line, start, p)?;
        idx += 1;
        start = p + 1;
    }
    if idx != N - 1 {
        return None;
    }
    out[idx] = unq(line, start, line.len())?;
    Some(out)
}

#[inline]
fn unq(line: &[u8], s: usize, e: usize) -> Option<(u32, u32)> {
    let f = &line[s..e];
    if memchr(b'"', f).is_none() {
        return Some((s as u32, (e - s) as u32));
    }
    if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' && memchr(b'"', &f[1..f.len() - 1]).is_none() {
        Some(((s + 1) as u32, (e - s - 2) as u32))
    } else {
        None
    }
}

// ------------------------------------------------------------------ timestamps
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    }
}
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let (x, y) = (b[i], b[i + 1]);
    if x.is_ascii_digit() && y.is_ascii_digit() {
        Some(((x - b'0') * 10 + (y - b'0')) as i64)
    } else {
        None
    }
}
/// "YYYY-MM-DDTHH:MM:SS" (first 19 bytes of b) -> epoch ms, None if not a real calendar time.
fn parse_base(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let da = d2(b, 8)?;
    let hh = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let ss = d2(b, 17)?;
    if !(1..=12).contains(&mo) || da < 1 || da > days_in_month(y, mo) || hh > 23 || mi > 59 || ss > 59 {
        return None;
    }
    Some((days_from_civil(y, mo, da) * 86_400 + hh * 3600 + mi * 60 + ss) * 1000)
}
/// F1 `…Z`, F2 `…±HH:MM`, F3 digits = epoch ms. Anything else is None.
fn parse_ts(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    if b.iter().all(|c| c.is_ascii_digit()) {
        let mut i = 0;
        while i < b.len() && b[i] == b'0' {
            i += 1;
        }
        if b.len() - i > 18 {
            return None;
        }
        let mut v: i64 = 0;
        for &c in &b[i..] {
            v = v * 10 + (c - b'0') as i64;
        }
        return Some(v);
    }
    if b.len() == 20 && b[19] == b'Z' {
        return parse_base(b);
    }
    if b.len() == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let base = parse_base(b)?;
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if b[19] == b'-' { base + off } else { base - off });
    }
    None
}
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() || !b.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < b.len() && b[i] == b'0' {
        i += 1;
    }
    let d = &b[i..];
    if d.is_empty() || d.len() > 8 {
        return None;
    }
    let mut v: u32 = 0;
    for &c in d {
        v = v * 10 + (c - b'0') as u32;
    }
    if v >= 1 && v <= 10_000_000 {
        Some(v)
    } else {
        None
    }
}

// ------------------------------------------------------------------ records
/// One parsed event line, before dedup. `valid` says whether it would survive validation.
#[derive(Clone, Copy)]
struct Rec {
    k0: u64,
    k1: u64, // 128-bit hash of event_id
    uh: u64, // hash of user_id
    uref: u64,
    ts: i64,
    ing: i64,
    price: u32,
    cat: u16,
    valid: bool,
}
/// A surviving event, already assigned to a person.
#[derive(Clone, Copy)]
struct Ev {
    pkey: u64,
    ts: i64,
    uref: u64, // reference to the person's name bytes
    price: u32, // 0 unless purchase
    cat: u16,
}
#[derive(Clone, Copy)]
struct ClickRaw {
    uh: u64,
    ts: i64,
    cid: u64,
    camp: u32,
}
#[derive(Clone, Copy)]
struct Click {
    pkey: u64,
    ts: i64,
    cid: u64,
    camp: u32,
}

struct Ctx {
    ws: i64,
    we: i64,
    prod: IdMap<u16>,
    unknown_cat: u16,
    camp: IdMap<u32>,
}

#[derive(Clone, Copy)]
struct Chunk {
    fid: u32,
    kind: u8, // 0 events, 1 clicks, 2 links
    start: usize,
    end: usize,
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

fn make_chunks(maps: &[Mmap], fid: usize, kind: u8, out: &mut Vec<Chunk>) {
    let data = &maps[fid][..];
    let n = data.len();
    if n == 0 {
        return;
    }
    let hdr_end = match memchr(b'\n', data) {
        Some(p) => p + 1,
        None => return,
    };
    let mut s = hdr_end;
    while s < n {
        let mut e = (s + CHUNK).min(n);
        if e < n {
            e = match memchr(b'\n', &data[e - 1..]) {
                Some(p) => e - 1 + p + 1,
                None => n,
            };
        }
        out.push(Chunk { fid: fid as u32, kind, start: s, end: e });
        s = e;
    }
}

#[inline]
fn for_lines(data: &[u8], start: usize, end: usize, mut f: impl FnMut(usize, &[u8])) {
    let mut pos = start;
    while pos < end {
        let (le, next) = match memchr(b'\n', &data[pos..end]) {
            Some(p) => (pos + p, pos + p + 1),
            None => (end, end),
        };
        f(pos, &data[pos..le]);
        pos = next;
    }
}

fn parse_event(line: &[u8], base: usize, fid: u64, ctx: &Ctx) -> Option<Rec> {
    let f = parse_fields::<7>(line)?;
    let fs = |i: usize| -> &[u8] { &line[f[i].0 as usize..(f[i].0 + f[i].1) as usize] };
    let eid = fs(0);
    if eid.is_empty() {
        return None;
    }
    let h = xxh3_128(eid);
    let uid = fs(1);
    let ts = parse_ts(fs(2));
    let ing = parse_ts(fs(3));
    let mut valid = !uid.is_empty() && ts.map_or(false, |t| t >= ctx.ws && t < ctx.we);
    match (ing, ts) {
        (Some(i), Some(t)) => {
            if i - t > DAY {
                valid = false;
            }
        }
        _ => valid = false,
    }
    let pid = fs(5);
    if pid.is_empty() {
        valid = false;
    }
    let mut price = 0u32;
    let mut cat = 0u16;
    match fs(4) {
        b"view" | b"cart" => {}
        b"purchase" => match parse_price(fs(6)) {
            Some(p) => {
                price = p;
                if !pid.is_empty() {
                    cat = *ctx.prod.get(&xxh3_64(pid)).unwrap_or(&ctx.unknown_cat);
                }
            }
            None => valid = false,
        },
        _ => valid = false,
    }
    Some(Rec {
        k0: (h >> 64) as u64,
        k1: h as u64,
        uh: if valid { xxh3_64(uid) } else { 0 },
        uref: pack(fid, base + f[1].0 as usize, f[1].1 as usize),
        ts: ts.unwrap_or(0),
        ing: ing.unwrap_or(i64::MAX),
        price,
        cat,
        valid,
    })
}

fn parse_click(line: &[u8], base: usize, fid: u64, ctx: &Ctx) -> Option<ClickRaw> {
    let f = parse_fields::<4>(line)?;
    let fs = |i: usize| -> &[u8] { &line[f[i].0 as usize..(f[i].0 + f[i].1) as usize] };
    if fs(0).is_empty() || fs(1).is_empty() {
        return None;
    }
    let ts = parse_ts(fs(3))?;
    if ts < ctx.ws - WEEK || ts >= ctx.we {
        return None;
    }
    let camp = *ctx.camp.get(&xxh3_64(fs(2)))?;
    Some(ClickRaw { uh: xxh3_64(fs(1)), ts, cid: pack(fid, base + f[0].0 as usize, f[0].1 as usize), camp })
}

struct AccA {
    ev: Vec<Vec<Rec>>,
    clicks: Vec<ClickRaw>,
    links: Vec<(u64, u64, u64, u64)>,
}

// ------------------------------------------------------------------ links
/// Union-find over link lines. Returns raw-id-hash -> (person key, name reference).
fn build_links(maps: &[Mmap], edges: &[Vec<(u64, u64, u64, u64)>]) -> IdMap<(u64, u64)> {
    let mut idx: IdMap<u32> = IdMap::default();
    let mut nodes: Vec<(u64, u64)> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = parent[x as usize];
        }
        x
    }
    let mut node = |h: u64, r: u64, nodes: &mut Vec<(u64, u64)>, parent: &mut Vec<u32>| -> u32 {
        *idx.entry(h).or_insert_with(|| {
            let n = nodes.len() as u32;
            nodes.push((h, r));
            parent.push(n);
            n
        })
    };
    for list in edges {
        for &(ha, ra, hb, rb) in list {
            let a = node(ha, ra, &mut nodes, &mut parent);
            let b = node(hb, rb, &mut nodes, &mut parent);
            let (ra_, rb_) = (find(&mut parent, a), find(&mut parent, b));
            if ra_ != rb_ {
                parent[ra_ as usize] = rb_;
            }
        }
    }
    let n = nodes.len();
    let mut best = vec![u32::MAX; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if best[r] == u32::MAX || bytes_of(maps, nodes[i].1) < bytes_of(maps, nodes[best[r] as usize].1) {
            best[r] = i as u32;
        }
    }
    let mut out: IdMap<(u64, u64)> = IdMap::default();
    out.reserve(n);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        let b = nodes[best[r] as usize];
        out.insert(nodes[i].0, (b.0, b.1));
    }
    out
}

// ------------------------------------------------------------------ per-person work
#[derive(Clone, Copy)]
struct URow {
    uref: u64,
    day: i32,
    sessions: u32,
    events: u32,
    rev: i64,
    top: u16,
}
struct Sess {
    start: i64,
    rev: i64,
    purch: u32,
    day: u16,
}
struct Acc {
    urows: Vec<URow>,
    cred_s: Vec<u32>,
    cred_p: Vec<u32>,
    cred_r: Vec<i64>,
    assisted: Vec<u32>,
    sess: Vec<Sess>,
    cs: Vec<(u32, i64)>,
    day_first: Vec<usize>,
    cats: Vec<(u16, i64)>,
}
struct Dims {
    base_day: i64,
    ndays: usize,
    organic: u32,
}
impl Acc {
    fn new(d: &Dims) -> Acc {
        let n = (d.organic as usize + 1) * d.ndays;
        Acc {
            urows: Vec::new(),
            cred_s: vec![0; n],
            cred_p: vec![0; n],
            cred_r: vec![0; n],
            assisted: vec![0; n],
            sess: Vec::new(),
            cs: Vec::new(),
            day_first: Vec::new(),
            cats: Vec::new(),
        }
    }
}

fn flush_day(acc: &mut Acc, uref: u64, day: i32, sessions: u32, events: u32, rev: i64) {
    let mut top = NO_CAT;
    let mut best = i64::MIN;
    for &(c, s) in &acc.cats {
        if s > best || (s == best && c < top) {
            best = s;
            top = c;
        }
    }
    acc.urows.push(URow { uref, day, sessions, events, rev, top });
    acc.cats.clear();
}

fn process_person(ev: &[Ev], clicks: &[Click], acc: &mut Acc, d: &Dims) {
    let nd = d.ndays;
    acc.sess.clear();
    acc.cats.clear();
    let uref = ev[0].uref;
    let (mut cur_day, mut d_sessions, mut d_events, mut d_rev) = (-1i64, 0u32, 0u32, 0i64);
    let mut prev_ts = 0i64;
    let mut events_in_sess = 0u32;
    let mut sess_events: Vec<u32> = Vec::new();
    let _ = &mut sess_events;
    for e in ev {
        if acc.sess.is_empty() || e.ts - prev_ts > GAP {
            let day = e.ts.div_euclid(DAY) - d.base_day;
            if day != cur_day {
                if cur_day >= 0 {
                    flush_day(acc, uref, (cur_day + d.base_day) as i32, d_sessions, d_events, d_rev);
                }
                cur_day = day;
                d_sessions = 0;
                d_events = 0;
                d_rev = 0;
            }
            d_sessions += 1;
            acc.sess.push(Sess { start: e.ts, rev: 0, purch: 0, day: day as u16 });
        }
        let _ = &mut events_in_sess;
        prev_ts = e.ts;
        d_events += 1;
        if e.price > 0 {
            let s = acc.sess.last_mut().unwrap();
            s.rev += e.price as i64;
            s.purch += 1;
            d_rev += e.price as i64;
            match acc.cats.iter_mut().find(|x| x.0 == e.cat) {
                Some(x) => x.1 += e.price as i64,
                None => acc.cats.push((e.cat, e.price as i64)),
            }
        }
    }
    flush_day(acc, uref, (cur_day + d.base_day) as i32, d_sessions, d_events, d_rev);

    // Credit: latest click at or before the session start (first click_id wins a ts tie,
    // because clicks are sorted by (ts, click_id)); older than 7 days => organic.
    let mut j = 0usize;
    let mut best: Option<(i64, u32)> = None;
    for s in &acc.sess {
        while j < clicks.len() && clicks[j].ts <= s.start {
            if best.map_or(true, |b| clicks[j].ts != b.0) {
                best = Some((clicks[j].ts, clicks[j].camp));
            }
            j += 1;
        }
        let camp = match best {
            Some((t, c)) if s.start - t <= WEEK => c,
            _ => d.organic,
        };
        let i = camp as usize * nd + s.day as usize;
        acc.cred_s[i] += 1;
        if s.purch > 0 {
            acc.cred_p[i] += 1;
        }
        acc.cred_r[i] += s.rev;
    }

    // Assists: merge each campaign's clicks into 7-day islands; every session starting
    // inside an island is assisted once by that campaign.
    if !clicks.is_empty() {
        acc.cs.clear();
        acc.cs.extend(clicks.iter().map(|c| (c.camp, c.ts)));
        acc.cs.sort_unstable();
        acc.day_first.clear();
        acc.day_first.resize(nd + 1, acc.sess.len());
        for dd in (0..nd).rev() {
            // first session index with day >= dd
            let idx = acc.sess.partition_point(|s| (s.day as usize) < dd);
            acc.day_first[dd] = idx;
        }
        let n = acc.cs.len();
        let mut i = 0;
        while i < n {
            let (c, lo) = acc.cs[i];
            let mut last = lo;
            i += 1;
            while i < n && acc.cs[i].0 == c && acc.cs[i].1 - last <= WEEK {
                last = acc.cs[i].1;
                i += 1;
            }
            let hi = last + WEEK;
            let i0 = acc.sess.partition_point(|s| s.start < lo);
            let i1 = acc.sess.partition_point(|s| s.start <= hi);
            if i0 < i1 {
                let d0 = acc.sess[i0].day as usize;
                let d1 = acc.sess[i1 - 1].day as usize;
                for dd in d0..=d1 {
                    let a = i0.max(acc.day_first[dd]);
                    let b = i1.min(acc.day_first[dd + 1]);
                    if b > a {
                        acc.assisted[c as usize * nd + dd] += (b - a) as u32;
                    }
                }
            }
        }
    }
}

// ------------------------------------------------------------------ parquet output
fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .build(),
    )
}

fn write_users(path: &Path, rows: &[URow], maps: &[Mmap], cats: &[Vec<u8>]) {
    let schema = Arc::new(
        parse_message_type(
            "message user_daily { REQUIRED BINARY user_id (STRING); REQUIRED INT32 day (DATE); \
             REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; \
             OPTIONAL BINARY top_category (STRING); }",
        )
        .unwrap(),
    );
    let mut w = SerializedFileWriter::new(File::create(path).unwrap(), schema, props()).unwrap();
    for part in rows.chunks(500_000) {
        let mut rg = w.next_row_group().unwrap();
        {
            let mut c = rg.next_column().unwrap().unwrap();
            let v: Vec<ByteArray> = part.iter().map(|r| ByteArray::from(bytes_of(maps, r.uref).to_vec())).collect();
            c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            c.close().unwrap();
        }
        {
            let mut c = rg.next_column().unwrap().unwrap();
            let v: Vec<i32> = part.iter().map(|r| r.day).collect();
            c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            c.close().unwrap();
        }
        for which in 0..3 {
            let mut c = rg.next_column().unwrap().unwrap();
            let v: Vec<i64> = part
                .iter()
                .map(|r| match which {
                    0 => r.sessions as i64,
                    1 => r.events as i64,
                    _ => r.rev,
                })
                .collect();
            c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            c.close().unwrap();
        }
        {
            let mut c = rg.next_column().unwrap().unwrap();
            let mut vals: Vec<ByteArray> = Vec::new();
            let mut defs: Vec<i16> = Vec::with_capacity(part.len());
            for r in part {
                if r.top == NO_CAT {
                    defs.push(0);
                } else {
                    defs.push(1);
                    vals.push(ByteArray::from(cats[r.top as usize].clone()));
                }
            }
            c.typed::<ByteArrayType>().write_batch(&vals, Some(&defs), None).unwrap();
            c.close().unwrap();
        }
        rg.close().unwrap();
    }
    w.close().unwrap();
}

struct CRow {
    camp: usize,
    day: i32,
    sessions: i64,
    purch: i64,
    rev: i64,
    assisted: i64,
}
fn write_campaigns(path: &Path, rows: &[CRow], names: &[Vec<u8>], channels: &[Vec<u8>]) {
    let schema = Arc::new(
        parse_message_type(
            "message campaign_daily { REQUIRED BINARY campaign_id (STRING); REQUIRED BINARY channel (STRING); \
             REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; \
             REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }",
        )
        .unwrap(),
    );
    let mut w = SerializedFileWriter::new(File::create(path).unwrap(), schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    for col in 0..7 {
        let mut c = rg.next_column().unwrap().unwrap();
        match col {
            0 | 1 => {
                let v: Vec<ByteArray> = rows
                    .iter()
                    .map(|r| ByteArray::from(if col == 0 { names[r.camp].clone() } else { channels[r.camp].clone() }))
                    .collect();
                c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }
            2 => {
                let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            _ => {
                let v: Vec<i64> = rows
                    .iter()
                    .map(|r| match col {
                        3 => r.sessions,
                        4 => r.purch,
                        5 => r.rev,
                        _ => r.assisted,
                    })
                    .collect();
                c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }
        }
        c.close().unwrap();
    }
    rg.close().unwrap();
    w.close().unwrap();
}

// ------------------------------------------------------------------ small CSVs
fn read_pairs(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = fs::read(path).unwrap_or_default();
    let mut out = Vec::new();
    let mut first = true;
    for line in data.split(|&b| b == b'\n') {
        if first {
            first = false;
            continue;
        }
        if let Some(f) = parse_fields::<2>(line) {
            let a = &line[f[0].0 as usize..(f[0].0 + f[0].1) as usize];
            let b = &line[f[1].0 as usize..(f[1].0 + f[1].1) as usize];
            if !a.is_empty() {
                out.push((a.to_vec(), b.to_vec()));
            }
        }
    }
    out
}

fn json_str(s: &str, key: &str) -> String {
    let k = format!("\"{}\"", key);
    let i = s.find(&k).expect("manifest key");
    let rest = &s[i + k.len()..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    rest[..q2].to_string()
}

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    if ncpu > 0 {
        rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().unwrap();
    }

    let manifest = fs::read_to_string(data.join("manifest.json")).unwrap();
    let ws = parse_ts(json_str(&manifest, "window_start").as_bytes()).unwrap();
    let we = parse_ts(json_str(&manifest, "window_end").as_bytes()).unwrap();

    // products -> category ids (sorted by name so id order == byte order); campaigns -> ids
    let products = read_pairs(&data.join("products.csv"));
    let mut cat_names: Vec<Vec<u8>> = products.iter().map(|p| p.1.clone()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    let cat_id = |n: &[u8]| cat_names.binary_search_by(|x| x.as_slice().cmp(n)).unwrap() as u16;
    let mut prod: IdMap<u16> = IdMap::default();
    for (p, c) in &products {
        prod.insert(xxh3_64(p), cat_id(c));
    }
    let unknown_cat = cat_id(b"unknown");
    let campaigns = read_pairs(&data.join("campaigns.csv"));
    let mut camp: IdMap<u32> = IdMap::default();
    let mut camp_names: Vec<Vec<u8>> = Vec::new();
    let mut camp_channels: Vec<Vec<u8>> = Vec::new();
    for (i, (c, ch)) in campaigns.iter().enumerate() {
        camp.insert(xxh3_64(c), i as u32);
        camp_names.push(c.clone());
        camp_channels.push(ch.clone());
    }
    let organic = camp_names.len() as u32;
    camp_names.push(b"organic".to_vec());
    camp_channels.push(b"organic".to_vec());
    let ctx = Ctx { ws, we, prod, unknown_cat, camp };

    // map every input file
    let mut paths: Vec<(u8, PathBuf)> = Vec::new();
    for (k, sub) in ["events", "clicks", "links"].iter().enumerate() {
        for p in csv_files(&data.join(sub)) {
            paths.push((k as u8, p));
        }
    }
    let maps: Vec<Mmap> = paths
        .iter()
        .map(|(_, p)| {
            let f = File::open(p).unwrap();
            if f.metadata().unwrap().len() == 0 {
                // mmap of an empty file fails: use an empty anonymous map instead
                memmap2::MmapMut::map_anon(1).unwrap().make_read_only().unwrap()
            } else {
                unsafe { Mmap::map(&f).unwrap() }
            }
        })
        .collect();
    let maps: &'static [Mmap] = Box::leak(maps.into_boxed_slice());
    let mut chunks: Vec<Chunk> = Vec::new();
    for (fid, (k, p)) in paths.iter().enumerate() {
        if std::fs::metadata(p).map(|m| m.len()).unwrap_or(0) > 0 {
            make_chunks(maps, fid, *k, &mut chunks);
        }
    }
    log(&t0, &format!("{} files, {} chunks", maps.len(), chunks.len()));

    // ---- A. parse everything
    let parts: Vec<AccA> = chunks
        .par_iter()
        .fold(
            || AccA { ev: vec![Vec::new(); NB], clicks: Vec::new(), links: Vec::new() },
            |mut acc, ch| {
                let data = &maps[ch.fid as usize][..];
                let fid = ch.fid as u64;
                match ch.kind {
                    0 => for_lines(data, ch.start, ch.end, |base, line| {
                        if let Some(r) = parse_event(line, base, fid, &ctx) {
                            acc.ev[bucket(r.k0)].push(r);
                        }
                    }),
                    1 => for_lines(data, ch.start, ch.end, |base, line| {
                        if let Some(c) = parse_click(line, base, fid, &ctx) {
                            acc.clicks.push(c);
                        }
                    }),
                    _ => for_lines(data, ch.start, ch.end, |base, line| {
                        if let Some(f) = parse_fields::<2>(line) {
                            let a = &line[f[0].0 as usize..(f[0].0 + f[0].1) as usize];
                            let b = &line[f[1].0 as usize..(f[1].0 + f[1].1) as usize];
                            if !a.is_empty() && !b.is_empty() && a != b {
                                acc.links.push((
                                    xxh3_64(a),
                                    pack(fid, base + f[0].0 as usize, f[0].1 as usize),
                                    xxh3_64(b),
                                    pack(fid, base + f[1].0 as usize, f[1].1 as usize),
                                ));
                            }
                        }
                    }),
                }
                acc
            },
        )
        .collect();
    log(&t0, "A: parsed");

    // ---- C. links -> person keys
    let link_edges: Vec<Vec<(u64, u64, u64, u64)>> = parts.iter().map(|p| p.links.clone()).collect();
    let link = build_links(maps, &link_edges);
    drop(link_edges);
    log(&t0, &format!("C: {} linked ids", link.len()));

    // clicks -> person buckets
    let click_parts: Vec<Vec<Vec<Click>>> = parts
        .par_iter()
        .map(|p| {
            let mut v: Vec<Vec<Click>> = vec![Vec::new(); NB];
            for c in &p.clicks {
                let pkey = link.get(&c.uh).map_or(c.uh, |x| x.0);
                v[bucket(pkey)].push(Click { pkey, ts: c.ts, cid: c.cid, camp: c.camp });
            }
            v
        })
        .collect();

    // ---- B. dedup per event-key bucket, then shuffle survivors to person buckets
    let ev_parts: Vec<Vec<Vec<Ev>>> = (0..NB)
        .into_par_iter()
        .map(|b| {
            let total: usize = parts.iter().map(|p| p.ev[b].len()).sum();
            let mut v: Vec<Rec> = Vec::with_capacity(total);
            for p in &parts {
                v.extend_from_slice(&p.ev[b]);
            }
            v.sort_unstable_by(|a, b| (a.k0, a.k1, a.ing).cmp(&(b.k0, b.k1, b.ing)));
            let mut res: Vec<Vec<Ev>> = vec![Vec::new(); NB];
            let mut i = 0;
            while i < v.len() {
                let r = v[i];
                let mut j = i + 1;
                while j < v.len() && v[j].k0 == r.k0 && v[j].k1 == r.k1 {
                    j += 1;
                }
                if r.valid {
                    let (pkey, nref) = match link.get(&r.uh) {
                        Some(&(a, b)) => (a, b),
                        None => (r.uh, r.uref),
                    };
                    res[bucket(pkey)].push(Ev { pkey, ts: r.ts, uref: nref, price: r.price, cat: r.cat });
                }
                i = j;
            }
            res
        })
        .collect();
    drop(parts);
    log(&t0, "B: deduped");

    // ---- D. per person: sessions, attribution, assists
    let base_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - base_day + 1) as usize;
    let dims = Dims { base_day, ndays, organic };
    let accs: Vec<Acc> = (0..NB)
        .into_par_iter()
        .fold(
            || Acc::new(&dims),
            |mut acc, p| {
                let total: usize = ev_parts.iter().map(|t| t[p].len()).sum();
                let mut evs: Vec<Ev> = Vec::with_capacity(total);
                for t in &ev_parts {
                    evs.extend_from_slice(&t[p]);
                }
                evs.sort_unstable_by_key(|e| (e.pkey, e.ts));
                let ctotal: usize = click_parts.iter().map(|t| t[p].len()).sum();
                let mut cl: Vec<Click> = Vec::with_capacity(ctotal);
                for t in &click_parts {
                    cl.extend_from_slice(&t[p]);
                }
                cl.sort_unstable_by(|a, b| {
                    a.pkey
                        .cmp(&b.pkey)
                        .then(a.ts.cmp(&b.ts))
                        .then_with(|| bytes_of(maps, a.cid).cmp(bytes_of(maps, b.cid)))
                });
                let (mut i, mut c0) = (0usize, 0usize);
                while i < evs.len() {
                    let pk = evs[i].pkey;
                    let mut j = i + 1;
                    while j < evs.len() && evs[j].pkey == pk {
                        j += 1;
                    }
                    while c0 < cl.len() && cl[c0].pkey < pk {
                        c0 += 1;
                    }
                    let mut c1 = c0;
                    while c1 < cl.len() && cl[c1].pkey == pk {
                        c1 += 1;
                    }
                    process_person(&evs[i..j], &cl[c0..c1], &mut acc, &dims);
                    c0 = c1;
                    i = j;
                }
                acc
            },
        )
        .collect();
    log(&t0, "D: sessions done");

    // ---- E. output
    fs::create_dir_all(out.join("user_daily")).unwrap();
    fs::create_dir_all(out.join("campaign_daily")).unwrap();
    accs.par_iter().enumerate().for_each(|(i, a)| {
        if !a.urows.is_empty() {
            write_users(&out.join("user_daily").join(format!("part-{:05}.parquet", i)), &a.urows, maps, &cat_names);
        }
    });
    let n = (organic as usize + 1) * ndays;
    let mut crows: Vec<CRow> = Vec::new();
    for idx in 0..n {
        let (mut s, mut p, mut r, mut a) = (0i64, 0i64, 0i64, 0i64);
        for acc in &accs {
            s += acc.cred_s[idx] as i64;
            p += acc.cred_p[idx] as i64;
            r += acc.cred_r[idx];
            a += acc.assisted[idx] as i64;
        }
        if s > 0 || a > 0 {
            crows.push(CRow { camp: idx / ndays, day: (base_day + (idx % ndays) as i64) as i32, sessions: s, purch: p, rev: r, assisted: a });
        }
    }
    write_campaigns(&out.join("campaign_daily").join("part-00000.parquet"), &crows, &camp_names, &camp_channels);
    if accs.iter().all(|a| a.urows.is_empty()) {
        write_users(&out.join("user_daily").join("part-00000.parquet"), &[], maps, &cat_names);
    }
    log(&t0, "E: written; done");
}
