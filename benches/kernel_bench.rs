//! Manual benchmark harness for the control-plane kernel (`harness = false`).
//!
//! Implements the workloads from `docs/BENCHMARKS.md`: transaction workloads
//! T1-T8, scale workloads (event history, terminal jobs, active vs. historical
//! partitions), and operational workloads O1-O9. Every report prints the full
//! environment metadata required by §1 and per-workload p50/p95/p99
//! distributions.
//!
//! Run with `cargo bench`. By default a reduced-scale profile runs so the
//! suite completes in minutes; set `KERNEL_BENCH_FULL=1` for the full §2.2
//! populations (1m events, 1m terminal jobs, ~1 GiB backup source) and the
//! full iteration counts (p99 at n<1000 is labeled indicative).
//!
//! Each run also emits a machine-readable report blob (§5) to
//! `target/kernel-bench/report.json`. Set `KERNEL_BENCH_COLD=1` to request a
//! cold-cache run: OS caches are dropped before open-time and every read
//! workload measurement when `/proc/sys/vm/drop_caches` is writable; without
//! root the run stays warm
//! and is labeled `cold mode unavailable (requires root)` per §1.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use minisqlite::{
    ClaimOutcome, ClaimRequest, ClaimedJob, CommitBatch, ControlPlaneStore, Durability, Event, Id,
    JobSpec, ProjectionPatch, Resolution, StoreBuilder,
};

const WARMUP: usize = 10;
const POPULATE_BATCH: usize = 1000;
const PAGE: usize = 256;

fn main() {
    // Cargo passes `--bench` when invoked via `cargo bench`; under
    // `cargo test --all-targets` the binary runs without it, so only verify
    // that the harness links and exit (mirrors criterion's behavior).
    if !std::env::args().any(|a| a == "--bench") {
        println!("kernel_bench: pass --bench (cargo bench) to run the suite");
        return;
    }

    if let Ok(workload) = std::env::var("KERNEL_BENCH_RSS_CHILD") {
        rss_child_main(&workload);
        return;
    }

    let profile = Profile::from_env();
    let root = bench_root();
    let durability_check = DurabilityCheck::detect(&root);
    print_environment(&root, &profile, &durability_check);

    transaction_workloads(&root, &profile);
    scale_event_history(&root, &profile);
    scale_terminal_jobs(&root, &profile);
    scale_partitions(&root, &profile);
    operational_projections(&root, &profile);
    live_backup(&root, &profile);

    // Remove fixture databases but keep the machine-readable report (§5).
    if let Ok(entries) = std::fs::read_dir(&root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let _ = std::fs::remove_dir_all(&path);
            } else {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    let report_path = write_json_report(&root, &profile, &durability_check);
    println!(
        "\nkernel_bench: JSON report written to {}",
        report_path.display()
    );
    println!("kernel_bench: done");
}

// ---------------------------------------------------------------------------
// Profile and environment report
// ---------------------------------------------------------------------------

struct Profile {
    full: bool,
    /// Measured iterations for single-op workloads (T1-T4, T6-T8, O2, O4).
    txn_iters: usize,
    /// Measured iterations for batch-claim workloads (T5 variants).
    claim_iters: usize,
    /// Drop OS caches before open-time and read-workload measurements
    /// (requires root).
    cold: bool,
    event_populations: Vec<u64>,
    terminal_job_populations: Vec<u64>,
    /// (active partitions, historical terminal partitions)
    partition_mixes: Vec<(u64, u64)>,
    backup_target_bytes: u64,
}

impl Profile {
    fn from_env() -> Self {
        let full = std::env::var("KERNEL_BENCH_FULL").is_ok_and(|v| v == "1");
        let cold = std::env::var("KERNEL_BENCH_COLD").is_ok_and(|v| v == "1") && can_drop_caches();
        if full {
            Self {
                full,
                txn_iters: 10_000,
                claim_iters: 1_000,
                cold,
                event_populations: vec![10_000, 100_000, 1_000_000],
                terminal_job_populations: vec![10_000, 100_000, 1_000_000],
                partition_mixes: vec![(100, 100), (100, 100_000), (1000, 1_000_000)],
                backup_target_bytes: 1 << 30,
            }
        } else {
            Self {
                full,
                txn_iters: 100,
                claim_iters: 30,
                cold,
                event_populations: vec![10_000, 100_000],
                terminal_job_populations: vec![10_000, 100_000],
                partition_mixes: vec![(100, 100), (100, 10_000), (1000, 100_000)],
                backup_target_bytes: 64 << 20,
            }
        }
    }
}

fn can_drop_caches() -> bool {
    std::fs::OpenOptions::new()
        .write(true)
        .open("/proc/sys/vm/drop_caches")
        .is_ok()
}

/// Drop OS page caches (cold-cache mode; requires root).
fn drop_caches() {
    let _ = Command::new("sync").status();
    let _ = std::fs::write("/proc/sys/vm/drop_caches", "3");
}

fn bench_root() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("kernel-bench");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create bench root");
    root
}

fn read_first_match(path: &str, key: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    text.lines()
        .find(|l| l.starts_with(key))
        .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
}

fn os_pretty_name() -> String {
    std::fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|t| {
            t.lines().find(|l| l.starts_with("PRETTY_NAME=")).map(|l| {
                l.trim_start_matches("PRETTY_NAME=")
                    .trim_matches('"')
                    .to_string()
            })
        })
        .unwrap_or_else(|| "unknown".into())
}

fn kernel_release() -> String {
    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".into())
}

/// Filesystem type and device backing `path`, from the longest mount-point
/// prefix in /proc/mounts.
fn filesystem_for(path: &Path) -> (String, String, String) {
    let mounts = match std::fs::read_to_string("/proc/mounts") {
        Ok(m) => m,
        Err(_) => return ("unknown".into(), "unknown".into(), "unknown".into()),
    };
    let target = path.to_string_lossy();
    let mut best: Option<(&str, &str, &str, &str)> = None;
    for line in mounts.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 4 {
            continue;
        }
        if target.starts_with(f[1]) && best.is_none_or(|b| f[1].len() > b.1.len()) {
            best = Some((f[0], f[1], f[2], f[3]));
        }
    }
    match best {
        Some((dev, _, fstype, opts)) => (fstype.into(), opts.into(), dev.into()),
        None => ("unknown".into(), "unknown".into(), "unknown".into()),
    }
}

fn device_class(device: &str) -> String {
    let name = device.trim_start_matches("/dev/");
    let base: String = name
        .trim_end_matches(|c: char| c.is_ascii_digit())
        .trim_end_matches('p')
        .to_string();
    for candidate in [name, base.as_str()] {
        let path = format!("/sys/block/{candidate}/queue/rotational");
        if let Ok(v) = std::fs::read_to_string(&path) {
            return if v.trim() == "0" {
                format!("non-rotational (SSD-class), device {device}")
            } else {
                format!("rotational (HDD-class), device {device}")
            };
        }
    }
    format!("unknown class, device {device}")
}

/// §1 suspicious-durability enforcement: flag environments that invalidate
/// durability-sensitive numbers (ephemeral/virtual filesystems, implausibly
/// fast fsync) instead of silently reporting them.
struct DurabilityCheck {
    suspicious: bool,
    reason: String,
    fsync_baseline: String,
}

impl DurabilityCheck {
    /// fsync medians below this on a dirty file imply the sync never reached
    /// durable media (tmpfs, lying cache, coalesced syncs).
    const SUSPICIOUS_FSYNC_US: f64 = 50.0;

    fn detect(root: &Path) -> Self {
        let mut reasons = Vec::new();
        let (fstype, _, _) = filesystem_for(root);
        const EPHEMERAL: &[&str] = &[
            "tmpfs",
            "ramfs",
            "overlay",
            "overlayfs",
            "9p",
            "nfs",
            "nfs4",
            "cifs",
        ];
        if EPHEMERAL.contains(&fstype.as_str()) {
            reasons.push(format!(
                "bench directory is on {fstype} (ephemeral/virtual filesystem)"
            ));
        }
        let fsync_baseline = match fsync_median_us(root) {
            Some(median_us) => {
                if median_us < Self::SUSPICIOUS_FSYNC_US {
                    reasons.push(format!(
                        "fsync median {median_us:.1}us is implausibly fast for durable storage"
                    ));
                }
                format!("median {median_us:.1}us over 20 syncs of a dirty 4 KiB file")
            }
            None => "unavailable (fsync probe failed)".to_string(),
        };
        Self {
            suspicious: !reasons.is_empty(),
            reason: if reasons.is_empty() {
                "none".to_string()
            } else {
                reasons.join("; ")
            },
            fsync_baseline,
        }
    }
}

/// Median latency of syncing a freshly rewritten 4 KiB file, in microseconds.
fn fsync_median_us(root: &Path) -> Option<f64> {
    use std::io::{Seek, Write};
    let path = root.join("fsync-probe.tmp");
    let mut file = std::fs::File::create(&path).ok()?;
    let block = [0u8; 4096];
    let mut samples = Vec::with_capacity(20);
    for _ in 0..20 {
        file.rewind().ok()?;
        file.write_all(&block).ok()?;
        let t = Instant::now();
        file.sync_all().ok()?;
        samples.push(t.elapsed());
    }
    drop(file);
    let _ = std::fs::remove_file(&path);
    samples.sort_unstable();
    Some(percentile(&samples, 0.50).as_secs_f64() * 1e6)
}

/// Measure the page size the store actually uses: open a probe store in the
/// bench root and read `PRAGMA page_size` from its database.
fn measured_page_size(root: &Path) -> String {
    let path = root.join("page-size-probe.db");
    let page_size = ControlPlaneStore::open(&path).ok().and_then(|_store| {
        rusqlite::Connection::open(&path)
            .and_then(|conn| conn.query_row("PRAGMA page_size", [], |row| row.get::<_, i64>(0)))
            .ok()
    });
    let _ = std::fs::remove_file(&path);
    match page_size {
        Some(n) => format!("{n} (measured via PRAGMA page_size)"),
        None => "unavailable (page-size probe failed)".to_string(),
    }
}

fn vmhwm_kb() -> Option<f64> {
    read_first_match("/proc/self/status", "VmHWM")
        .and_then(|kb| kb.trim_end_matches(" kB").trim().parse().ok())
}

fn env_field(key: &str, value: &str) {
    println!("{:<17}{value}", format!("{key}:"));
    JSON_ENV
        .lock()
        .expect("env lock")
        .push((key.to_string(), value.to_string()));
}

fn print_environment(root: &Path, profile: &Profile, durability_check: &DurabilityCheck) {
    let (fstype, opts, device) = filesystem_for(root);
    println!("== kernel_bench environment report (docs/BENCHMARKS.md §1) ==");
    env_field(
        "CPU",
        &format!(
            "{} ({} logical cores)",
            read_first_match("/proc/cpuinfo", "model name").unwrap_or_else(|| "unknown".into()),
            std::thread::available_parallelism().map_or(0, std::num::NonZero::get)
        ),
    );
    env_field(
        "RAM",
        &read_first_match("/proc/meminfo", "MemTotal").unwrap_or_else(|| "unknown".into()),
    );
    env_field(
        "OS",
        &format!("{}, kernel {}", os_pretty_name(), kernel_release()),
    );
    env_field("SQLite version", rusqlite::version());
    env_field("Filesystem", &format!("{fstype}, {opts}"));
    env_field("Storage device", &device_class(&device));
    env_field("Page size", &measured_page_size(root));
    env_field(
        "WAL state",
        "WAL on (set at open), fresh DB per fixture; per-group sizes on 'db state' lines",
    );
    env_field(
        "DB size",
        "per scale section / workload group, on 'db state' lines below",
    );
    let cache = if profile.cold {
        "cold (drop_caches before open-time and every read workload)".to_string()
    } else if std::env::var("KERNEL_BENCH_COLD").is_ok_and(|v| v == "1") {
        "warm; cold mode unavailable (requires root to write /proc/sys/vm/drop_caches)".to_string()
    } else {
        "warm (in-process, no cache drop; set KERNEL_BENCH_COLD=1 for cold)".to_string()
    };
    env_field("Cache state", &cache);
    env_field("Bench DB root", &root.display().to_string());
    env_field(
        "Profile",
        &format!(
            "{} (KERNEL_BENCH_FULL={})",
            if profile.full {
                "full §2.2 populations"
            } else {
                "reduced populations"
            },
            u8::from(profile.full)
        ),
    );
    env_field(
        "Iterations",
        &format!(
            "txn={} claim-batch={} (p99 at n<1000 labeled indicative)",
            profile.txn_iters, profile.claim_iters
        ),
    );
    env_field(
        "Warm-up iters",
        &format!("{WARMUP} (excluded from measurements)"),
    );
    env_field(
        "Durability",
        "per-workload, printed on each line (strict=FULL, relaxed=NORMAL)",
    );
    env_field(
        "Peak RSS",
        "per workload, sampled in a fresh child process (VmHWM), printed below",
    );
    env_field("Fsync baseline", &durability_check.fsync_baseline);
    env_field(
        "Suspicious durability",
        &format!(
            "{} (reason: {})",
            durability_check.suspicious, durability_check.reason
        ),
    );
}

// ---------------------------------------------------------------------------
// Measurement helpers
// ---------------------------------------------------------------------------

/// Deterministic ID generator: sequential u128 values in a private range.
struct IdGen(u128);

impl IdGen {
    fn new(range: u128) -> Self {
        Self(range << 64)
    }

    fn next(&mut self) -> Id {
        self.0 += 1;
        Id::from(self.0)
    }
}

/// Deterministic logical clock (milliseconds).
struct Clock(i64);

impl Clock {
    fn new() -> Self {
        Self(1_000)
    }

    fn tick(&mut self) -> i64 {
        self.0 += 1;
        self.0
    }
}

fn fmt_dur(d: Duration) -> String {
    let us = d.as_secs_f64() * 1e6;
    if us >= 10_000.0 {
        format!("{:.2}ms", us / 1000.0)
    } else {
        format!("{us:.0}us")
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn report(name: &str, samples: &mut [Duration]) {
    samples.sort_unstable();
    let indicative = if samples.len() < 1000 {
        " (p99 indicative: n<1000)"
    } else {
        ""
    };
    println!(
        "{name:<44} n={:<7} p50={:<9} p95={:<9} p99={:<9} min={:<9} max={}{indicative}",
        samples.len(),
        fmt_dur(percentile(samples, 0.50)),
        fmt_dur(percentile(samples, 0.95)),
        fmt_dur(percentile(samples, 0.99)),
        fmt_dur(samples[0]),
        fmt_dur(samples[samples.len() - 1]),
    );
    json_entry(&format!(
        "{{\"type\":\"distribution\",\"name\":\"{}\",\"n\":{},\"p50_us\":{:.1},\"p95_us\":{:.1},\"p99_us\":{:.1},\"min_us\":{:.1},\"max_us\":{:.1},\"p99_indicative\":{}}}",
        json_escape(name),
        samples.len(),
        percentile(samples, 0.50).as_secs_f64() * 1e6,
        percentile(samples, 0.95).as_secs_f64() * 1e6,
        percentile(samples, 0.99).as_secs_f64() * 1e6,
        samples[0].as_secs_f64() * 1e6,
        samples[samples.len() - 1].as_secs_f64() * 1e6,
        samples.len() < 1000,
    ));
}

/// Run `warmup` unmeasured then `iters` measured iterations and report.
fn run_workload(name: &str, warmup: usize, iters: usize, mut f: impl FnMut()) {
    for _ in 0..warmup {
        f();
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        f();
        samples.push(t.elapsed());
    }
    report(name, &mut samples);
}

/// Like `run_workload`, but runs an untimed `prep` step before every
/// iteration (warm-up included) so setup work such as re-acking previously
/// claimed jobs stays out of the samples.
fn run_workload_prepped<S>(
    name: &str,
    warmup: usize,
    iters: usize,
    state: &mut S,
    mut prep: impl FnMut(&mut S),
    mut f: impl FnMut(&mut S),
) {
    for _ in 0..warmup {
        prep(state);
        f(state);
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        prep(state);
        let t = Instant::now();
        f(state);
        samples.push(t.elapsed());
    }
    report(name, &mut samples);
}

fn report_single(name: &str, d: Duration, extra: &str) {
    println!("{name:<44} n=1       time={:<9} {extra}", fmt_dur(d));
    json_entry(&format!(
        "{{\"type\":\"single\",\"name\":\"{}\",\"time_us\":{:.1},\"extra\":\"{}\"}}",
        json_escape(name),
        d.as_secs_f64() * 1e6,
        json_escape(extra),
    ));
}

// ---------------------------------------------------------------------------
// §1 per-group DB/WAL state, §5 fresh-process peak RSS, §5 JSON report blob
// ---------------------------------------------------------------------------

static JSON_ENV: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());
static JSON_ENTRIES: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn json_entry(entry: &str) {
    JSON_ENTRIES
        .lock()
        .expect("entries lock")
        .push(entry.to_string());
}

/// Write the §5 machine-readable report blob for this run.
fn write_json_report(
    root: &Path,
    profile: &Profile,
    durability_check: &DurabilityCheck,
) -> PathBuf {
    let mut out = String::from("{\n  \"environment\": {\n");
    let env = JSON_ENV.lock().expect("env lock");
    for (i, (key, value)) in env.iter().enumerate() {
        let comma = if i + 1 < env.len() { "," } else { "" };
        out.push_str(&format!(
            "    \"{}\": \"{}\"{comma}\n",
            json_escape(key),
            json_escape(value)
        ));
    }
    out.push_str(&format!(
        "  }},\n  \"profile\": \"{}\",\n  \"suspicious_durability\": {},\n  \"suspicious_durability_reason\": \"{}\",\n  \"entries\": [\n",
        if profile.full { "full" } else { "reduced" },
        durability_check.suspicious,
        json_escape(&durability_check.reason),
    ));
    let entries = JSON_ENTRIES.lock().expect("entries lock");
    for (i, entry) in entries.iter().enumerate() {
        let comma = if i + 1 < entries.len() { "," } else { "" };
        out.push_str(&format!("    {entry}{comma}\n"));
    }
    out.push_str("  ]\n}\n");
    std::fs::create_dir_all(root).expect("recreate bench root");
    let path = root.join("report.json");
    std::fs::write(&path, out).expect("write JSON report");
    path
}

fn fmt_mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// Print the §1 DB-size and WAL-state fields for one workload group: on-disk
/// DB file size and `-wal` file size, read from file metadata only so state
/// reporting never mutates the fixture between measurements.
fn print_db_state(context: &str, path: &Path) {
    let db_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let wal_path = PathBuf::from(format!("{}-wal", path.display()));
    let wal_bytes = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    println!(
        "    db state [{context}]: db={} wal={}",
        fmt_mib(db_bytes),
        fmt_mib(wal_bytes),
    );
    json_entry(&format!(
        "{{\"type\":\"db_state\",\"context\":\"{}\",\"db_bytes\":{db_bytes},\"wal_bytes\":{wal_bytes}}}",
        json_escape(context),
    ));
}

/// §5 peak RSS: re-invoke this bench binary as a fresh child process running
/// exactly one workload and report that child's `VmHWM`, so workloads do not
/// contaminate each other's high-water mark.
fn print_fresh_process_rss(root: &Path, label: &str, workload: &str, db: Option<&Path>) {
    let exe = std::env::current_exe().expect("bench executable path");
    let mut cmd = Command::new(exe);
    cmd.arg("--bench")
        .env("KERNEL_BENCH_RSS_CHILD", workload)
        .env("KERNEL_BENCH_ROOT", root);
    if let Some(db) = db {
        cmd.env("KERNEL_BENCH_DB", db);
    }
    let output = cmd.output().expect("spawn RSS child process");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let rss = stdout
        .lines()
        .find_map(|l| l.strip_prefix("child_vmhwm_kb="))
        .and_then(|v| v.trim().parse::<f64>().ok());
    let text = match rss {
        Some(kb) if output.status.success() => format!("{:.1} MiB", kb / 1024.0),
        _ => "unavailable".to_string(),
    };
    println!("    peak RSS (fresh process) [{label}]: {text}");
    json_entry(&format!(
        "{{\"type\":\"peak_rss\",\"label\":\"{}\",\"workload\":\"{}\",\"rss\":\"{}\"}}",
        json_escape(label),
        json_escape(workload),
        json_escape(&text),
    ));
}

/// The fixture database an `o*` RSS child reads (`KERNEL_BENCH_DB`).
fn rss_child_db() -> PathBuf {
    PathBuf::from(std::env::var("KERNEL_BENCH_DB").expect("KERNEL_BENCH_DB"))
}

/// Child-process entry point for fresh-process RSS sampling: run one workload
/// and print this process's `VmHWM` in KiB. Every T workload runs against a
/// fresh child-local DB; every O workload reads the shared fixture passed via
/// `KERNEL_BENCH_DB`.
fn rss_child_main(workload: &str) {
    let root = PathBuf::from(std::env::var("KERNEL_BENCH_ROOT").expect("KERNEL_BENCH_ROOT"));
    let dir = root.join("rss-child");
    std::fs::create_dir_all(&dir).expect("mkdir rss-child");
    let mut ids = IdGen::new(99);
    let mut clock = Clock::new();
    let iters = (WARMUP + 100) as u64;
    let durability = if workload.ends_with("-strict") {
        Durability::Strict
    } else {
        Durability::Relaxed
    };
    let fresh_db = |name: &str| {
        let path = dir.join(format!("{name}.db"));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        path
    };
    let kind = workload.split('-').next().unwrap_or(workload);
    match kind {
        "t1" => {
            let store = open_store(&fresh_db(workload), durability);
            for _ in 0..iters {
                let batch = CommitBatch::new(ids.next(), clock.tick())
                    .append_event(event(&mut ids, &mut clock, "s"));
                store.commit(&batch).expect("child T1 commit");
            }
        }
        "t2" => {
            let store = open_store(&fresh_db(workload), durability);
            for n in 0..iters {
                let batch = CommitBatch::new(ids.next(), clock.tick())
                    .append_event(event(&mut ids, &mut clock, "s"))
                    .apply_projection_patch(ProjectionPatch::new("proj", n).put(b"key", b"value"));
                store.commit(&batch).expect("child T2 commit");
            }
        }
        "t3" => {
            let store = open_store(&fresh_db(workload), durability);
            for n in 0..iters {
                let job = JobSpec::reconcilable(ids.next(), "q", format!("p{n}"), b"{}".to_vec());
                let batch = CommitBatch::new(ids.next(), clock.tick())
                    .append_event(event(&mut ids, &mut clock, "s"))
                    .apply_projection_patch(ProjectionPatch::new("proj", n).put(b"key", b"value"))
                    .enqueue_job(job);
                store.commit(&batch).expect("child T3 commit");
            }
        }
        "t4" => {
            let store = open_store(&fresh_db(workload), durability);
            populate_jobs(&store, &mut ids, &mut clock, "q", "p", iters, iters);
            for _ in 0..iters {
                let jobs = claim(&store, "q", clock.tick(), 1);
                assert_eq!(jobs.len(), 1, "child T4 expected one claimed job");
            }
        }
        "t5" => {
            let store = open_store(&fresh_db(workload), durability);
            let rounds = 5u64;
            populate_jobs(&store, &mut ids, &mut clock, "q", "p", 100 * rounds, 100);
            for _ in 0..rounds {
                let jobs = claim(&store, "q", clock.tick(), 100);
                ack_all(&store, &mut ids, &mut clock, &jobs);
            }
        }
        "t6" => {
            let store = open_store(&fresh_db(workload), durability);
            populate_jobs(&store, &mut ids, &mut clock, "q", "p", iters, iters);
            let mut version = 0u64;
            loop {
                let jobs = claim(&store, "q", clock.tick(), 100);
                if jobs.is_empty() {
                    break;
                }
                for job in jobs {
                    let batch = CommitBatch::new(ids.next(), clock.tick())
                        .append_event(event(&mut ids, &mut clock, "s"))
                        .apply_projection_patch(
                            ProjectionPatch::new("proj", version).put(b"key", b"done"),
                        )
                        .acknowledge_job(job.job_id, job.lease_token, None);
                    store.commit(&batch).expect("child T6 commit");
                    version += 1;
                }
            }
        }
        "t7" => {
            let store = open_store(&fresh_db(workload), durability);
            populate_jobs(&store, &mut ids, &mut clock, "q", "p", 1, 1);
            let job = claim(&store, "q", clock.tick(), 1)
                .pop()
                .expect("child T7 claim");
            let mut expiry = job.lease_expires_at_ms;
            for _ in 0..iters {
                expiry += 1_000;
                store
                    .extend_lease(job.job_id, job.lease_token, expiry, clock.tick())
                    .expect("child T7 extend lease");
            }
        }
        "t8" => {
            let store = open_store(&fresh_db(workload), durability);
            populate_jobs(&store, &mut ids, &mut clock, "q", "p", iters, iters);
            let mut leased = Vec::new();
            loop {
                let request = ClaimRequest {
                    queue: "q".into(),
                    worker_id: "bench-worker".into(),
                    now_ms: clock.tick(),
                    lease_ms: 1,
                    limit: 1000,
                    partition_key: None,
                };
                let jobs = claimed_jobs(store.claim_jobs(&request).expect("child T8 claim"));
                if jobs.is_empty() {
                    break;
                }
                leased.extend(jobs.into_iter().map(|j| j.job_id));
            }
            clock.0 += 3_600_000; // expire every short lease
            for _ in 0..(iters / 32 + 2) {
                // Maintenance repairs at most 64 expired leases per claim call.
                let request = ClaimRequest {
                    queue: "q".into(),
                    worker_id: "bench-worker".into(),
                    now_ms: clock.tick(),
                    lease_ms: 1,
                    limit: 0,
                    partition_key: None,
                };
                store.claim_jobs(&request).expect("child T8 maintenance");
            }
            for job_id in leased {
                let job = store
                    .job(job_id)
                    .expect("child T8 job lookup")
                    .expect("child T8 job");
                if job.state == minisqlite::JobState::Uncertain {
                    let batch = CommitBatch::new(ids.next(), clock.tick())
                        .resolve_uncertain_job(job_id, Resolution::MarkSucceeded);
                    store.commit(&batch).expect("child T8 resolve");
                }
            }
        }
        "o1" => {
            let store = ControlPlaneStore::open(rss_child_db()).expect("child O1 open");
            store.stream_version("s0").expect("child O1 first read");
        }
        "o2" => {
            let store = open_store(&rss_child_db(), Durability::Relaxed);
            for _ in 0..iters {
                let events = store
                    .stream_events("s0", 1, 100)
                    .expect("child O2 stream read");
                assert_eq!(events.len(), 100, "child O2 expected 100 events");
            }
        }
        "o3" => {
            let store = open_store(&rss_child_db(), Durability::Relaxed);
            let mut cursor = 0u64;
            loop {
                let events = store.events_after(cursor, PAGE).expect("child O3 page");
                match events.last() {
                    Some(last) if events.len() == PAGE => cursor = last.global_sequence,
                    _ => break,
                }
            }
        }
        "o4" => {
            let store = open_store(&rss_child_db(), Durability::Relaxed);
            for n in 0..iters {
                let key = format!("key/{:08}", (n * 7919) % 10_000);
                let value = store
                    .projection_get("proj", key.as_bytes())
                    .expect("child O4 get");
                assert!(value.is_some(), "child O4 expected a value");
            }
        }
        "o5" => {
            let store = open_store(&rss_child_db(), Durability::Relaxed);
            let mut after: Option<Vec<u8>> = None;
            loop {
                let page = store
                    .projection_scan_prefix_page("proj", b"key/", after.as_deref(), PAGE)
                    .expect("child O5 page");
                match page.last() {
                    Some(last) if page.len() == PAGE => after = Some(last.key.clone()),
                    _ => break,
                }
            }
        }
        "o6" => {
            let store = open_store(&rss_child_db(), Durability::Relaxed);
            let mut cursor = 0u64;
            loop {
                let (jobs, next) = store
                    .jobs_page(Some("histq"), None, cursor, PAGE)
                    .expect("child O6 jobs page");
                if jobs.len() < PAGE {
                    break;
                }
                cursor = next;
            }
        }
        "o7" => {
            let store = open_store(&rss_child_db(), Durability::Relaxed);
            let dest = dir.join("o7-backup.db");
            store.backup(&dest, true).expect("child O7 backup");
        }
        "o8" => {
            let store = open_store(&rss_child_db(), Durability::Relaxed);
            store.verify().expect("child O8 verify");
        }
        "o9" => {
            let store = open_store(&rss_child_db(), Durability::Relaxed);
            let export = store.diagnostic_export().expect("child O9 export");
            assert!(!export.is_empty(), "child O9 export empty");
        }
        other => panic!("unknown RSS child workload: {other}"),
    }
    println!(
        "child_vmhwm_kb={}",
        vmhwm_kb().expect("child VmHWM unavailable")
    );
}

fn open_store(path: &Path, durability: Durability) -> ControlPlaneStore {
    StoreBuilder::new(path)
        .durability(durability)
        .open()
        .expect("open store")
}

fn durability_label(d: Durability) -> &'static str {
    match d {
        Durability::Strict => "strict",
        Durability::Relaxed => "relaxed",
    }
}

fn event(ids: &mut IdGen, clock: &mut Clock, stream: &str) -> Event {
    Event::with_json_payload(ids.next(), stream, "bench", clock.tick(), b"{\"n\":1}")
}

fn claimed_jobs(outcome: ClaimOutcome) -> Vec<ClaimedJob> {
    match outcome {
        ClaimOutcome::Committed(claims) => claims.into_jobs(),
        _ => Vec::new(),
    }
}

fn claim(store: &ControlPlaneStore, queue: &str, now_ms: i64, limit: usize) -> Vec<ClaimedJob> {
    let request = ClaimRequest {
        queue: queue.into(),
        worker_id: "bench-worker".into(),
        now_ms,
        lease_ms: 3_600_000,
        limit,
        partition_key: None,
    };
    claimed_jobs(store.claim_jobs(&request).expect("claim jobs"))
}

fn ack_all(store: &ControlPlaneStore, ids: &mut IdGen, clock: &mut Clock, jobs: &[ClaimedJob]) {
    for chunk in jobs.chunks(1000) {
        let mut batch = CommitBatch::new(ids.next(), clock.tick());
        for job in chunk {
            batch = batch.acknowledge_job(job.job_id, job.lease_token, None);
        }
        store.commit(&batch).expect("ack jobs");
    }
}

// ---------------------------------------------------------------------------
// Fixture population
// ---------------------------------------------------------------------------

/// Append `count` small events round-robin over `streams` streams, in relaxed
/// batches of up to `POPULATE_BATCH` events per commit.
fn populate_events(
    store: &ControlPlaneStore,
    ids: &mut IdGen,
    clock: &mut Clock,
    count: u64,
    streams: u64,
) {
    let mut written = 0u64;
    while written < count {
        let n = POPULATE_BATCH.min((count - written) as usize);
        let mut batch = CommitBatch::new(ids.next(), clock.tick());
        for i in 0..n {
            let stream = format!("s{}", (written + i as u64) % streams);
            batch = batch.append_event(event(ids, clock, &stream));
        }
        store.commit(&batch).expect("populate events");
        written += n as u64;
    }
}

/// Enqueue `count` jobs spread over `partitions` partitions named
/// `{prefix}{i}`, in relaxed batches.
fn populate_jobs(
    store: &ControlPlaneStore,
    ids: &mut IdGen,
    clock: &mut Clock,
    queue: &str,
    prefix: &str,
    count: u64,
    partitions: u64,
) {
    let mut written = 0u64;
    while written < count {
        let n = POPULATE_BATCH.min((count - written) as usize);
        let mut batch = CommitBatch::new(ids.next(), clock.tick());
        for i in 0..n {
            let partition = format!("{prefix}{}", (written + i as u64) % partitions);
            batch = batch.enqueue_job(JobSpec::reconcilable(
                ids.next(),
                queue,
                partition,
                b"{}".to_vec(),
            ));
        }
        store.commit(&batch).expect("populate jobs");
        written += n as u64;
    }
}

/// Drive every pending job in `queue` to the terminal Succeeded state via
/// claim + ack rounds (one job per partition per round).
fn terminalize_queue(store: &ControlPlaneStore, ids: &mut IdGen, clock: &mut Clock, queue: &str) {
    loop {
        let jobs = claim(store, queue, clock.tick(), 1000);
        if jobs.is_empty() {
            break;
        }
        ack_all(store, ids, clock, &jobs);
    }
}

// ---------------------------------------------------------------------------
// §2.1 Transaction workloads (T1-T8)
// ---------------------------------------------------------------------------

fn transaction_workloads(root: &Path, profile: &Profile) {
    println!("\n== §2.1 transaction workloads (fresh file-backed DB per workload) ==");
    for durability in [Durability::Strict, Durability::Relaxed] {
        let label = durability_label(durability);
        commit_workloads(root, profile, durability, label);
        claim_workloads(root, profile, durability, label);
        lifecycle_workloads(root, profile, durability, label);
        for t in 1..=8 {
            print_fresh_process_rss(
                root,
                &format!("T{t} {label}"),
                &format!("t{t}-{label}"),
                None,
            );
        }
    }
}

fn commit_workloads(root: &Path, profile: &Profile, durability: Durability, label: &str) {
    let dir = root.join(format!("txn-{label}"));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut ids = IdGen::new(1);
    let mut clock = Clock::new();

    let store = open_store(&dir.join("t1.db"), durability);
    run_workload(
        &format!("T1 commit event only [{label}]"),
        WARMUP,
        profile.txn_iters,
        || {
            let batch = CommitBatch::new(ids.next(), clock.tick())
                .append_event(event(&mut ids, &mut clock, "s"));
            store.commit(&batch).expect("T1 commit");
        },
    );

    let store = open_store(&dir.join("t2.db"), durability);
    let mut version = 0u64;
    run_workload(
        &format!("T2 event + projection [{label}]"),
        WARMUP,
        profile.txn_iters,
        || {
            let batch = CommitBatch::new(ids.next(), clock.tick())
                .append_event(event(&mut ids, &mut clock, "s"))
                .apply_projection_patch(
                    ProjectionPatch::new("proj", version).put(b"key", b"value"),
                );
            store.commit(&batch).expect("T2 commit");
            version += 1;
        },
    );

    let store = open_store(&dir.join("t3.db"), durability);
    let mut version = 0u64;
    let mut n = 0u64;
    run_workload(
        &format!("T3 event + projection + job [{label}]"),
        WARMUP,
        profile.txn_iters,
        || {
            let job = JobSpec::reconcilable(ids.next(), "q", format!("p{n}"), b"{}".to_vec());
            let batch = CommitBatch::new(ids.next(), clock.tick())
                .append_event(event(&mut ids, &mut clock, "s"))
                .apply_projection_patch(ProjectionPatch::new("proj", version).put(b"key", b"value"))
                .enqueue_job(job);
            store.commit(&batch).expect("T3 commit");
            version += 1;
            n += 1;
        },
    );
    print_db_state(&format!("T1-T3 {label}"), &dir.join("t3.db"));
}

fn claim_workloads(root: &Path, profile: &Profile, durability: Durability, label: &str) {
    let dir = root.join(format!("claim-{label}"));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut ids = IdGen::new(2);
    let mut clock = Clock::new();

    // T4: one pending job per partition; every claim leases the next partition.
    let path = dir.join("t4.db");
    {
        let setup = open_store(&path, Durability::Relaxed);
        let total = (WARMUP + profile.txn_iters) as u64;
        populate_jobs(&setup, &mut ids, &mut clock, "q", "p", total, total);
    }
    let store = open_store(&path, durability);
    run_workload(
        &format!("T4 claim one job [{label}]"),
        WARMUP,
        profile.txn_iters,
        || {
            let jobs = claim(&store, "q", clock.tick(), 1);
            assert_eq!(jobs.len(), 1, "T4 expected one claimed job");
        },
    );

    // T5: 100 partitions with enough depth for every iteration; jobs are acked
    // (untimed, in the prep step) between iterations so each partition head is
    // pending again and samples measure the claim alone.
    let path = dir.join("t5.db");
    {
        let setup = open_store(&path, Durability::Relaxed);
        let total = (WARMUP + profile.claim_iters) as u64 * 100;
        populate_jobs(&setup, &mut ids, &mut clock, "q", "p", total, 100);
    }
    let store = open_store(&path, durability);
    let mut state = (&mut ids, &mut clock, Vec::<ClaimedJob>::new());
    run_workload_prepped(
        &format!("T5 claim 100 jobs / 100 partitions [{label}]"),
        WARMUP,
        profile.claim_iters,
        &mut state,
        |s| {
            ack_all(&store, s.0, s.1, &s.2);
            s.2.clear();
        },
        |s| {
            s.2 = claim(&store, "q", s.1.tick(), 100);
            assert_eq!(s.2.len(), 100, "T5 expected 100 claimed jobs");
        },
    );
    print_db_state(&format!("T4-T5 {label}"), &path);
}

fn lifecycle_workloads(root: &Path, profile: &Profile, durability: Durability, label: &str) {
    let dir = root.join(format!("life-{label}"));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut ids = IdGen::new(3);
    let mut clock = Clock::new();

    // T6: completion event + projection update + ack in one batch, against
    // jobs claimed outside the timed section (refill happens in the untimed
    // prep step).
    let path = dir.join("t6.db");
    let total = (WARMUP + profile.txn_iters) as u64;
    {
        let setup = open_store(&path, Durability::Relaxed);
        populate_jobs(&setup, &mut ids, &mut clock, "q", "p", total, total);
    }
    let store = open_store(&path, durability);
    let mut state = (&mut ids, &mut clock, Vec::<ClaimedJob>::new(), 0u64);
    run_workload_prepped(
        &format!("T6 completion + projection + ack [{label}]"),
        WARMUP,
        profile.txn_iters,
        &mut state,
        |s| {
            if s.2.is_empty() {
                s.2 = claim(&store, "q", s.1.tick(), 100);
                s.2.reverse();
            }
        },
        |s| {
            let job = s.2.pop().expect("T6 claimed job");
            let batch = CommitBatch::new(s.0.next(), s.1.tick())
                .append_event(event(s.0, s.1, "s"))
                .apply_projection_patch(ProjectionPatch::new("proj", s.3).put(b"key", b"done"))
                .acknowledge_job(job.job_id, job.lease_token, None);
            store.commit(&batch).expect("T6 commit");
            s.3 += 1;
        },
    );

    // T7: repeated lease extension on one leased job.
    let path = dir.join("t7.db");
    {
        let setup = open_store(&path, Durability::Relaxed);
        populate_jobs(&setup, &mut ids, &mut clock, "q", "p", 1, 1);
    }
    let store = open_store(&path, durability);
    let job = claim(&store, "q", clock.tick(), 1).pop().expect("T7 claim");
    let mut expiry = job.lease_expires_at_ms;
    run_workload(
        &format!("T7 lease extension [{label}]"),
        WARMUP,
        profile.txn_iters,
        || {
            expiry += 1_000;
            store
                .extend_lease(job.job_id, job.lease_token, expiry, clock.tick())
                .expect("T7 extend lease");
        },
    );

    // T8: resolve Uncertain -> Succeeded. Jobs are made uncertain by claiming
    // with a short lease and letting maintenance repair the expiry.
    let path = dir.join("t8.db");
    let total = WARMUP + profile.txn_iters;
    let mut uncertain: Vec<Id> = Vec::new();
    {
        let setup = open_store(&path, Durability::Relaxed);
        populate_jobs(
            &setup,
            &mut ids,
            &mut clock,
            "q",
            "p",
            total as u64,
            total as u64,
        );
        let mut short_leased = Vec::new();
        loop {
            let request = ClaimRequest {
                queue: "q".into(),
                worker_id: "bench-worker".into(),
                now_ms: clock.tick(),
                lease_ms: 1,
                limit: 1000,
                partition_key: None,
            };
            let jobs = claimed_jobs(setup.claim_jobs(&request).expect("T8 claim"));
            if jobs.is_empty() && short_leased.len() >= total {
                break;
            }
            short_leased.extend(jobs.into_iter().map(|j| j.job_id));
        }
        clock.0 += 3_600_000; // expire every short lease
        while uncertain.len() < total {
            // Maintenance repairs at most 64 expired leases per claim call.
            let request = ClaimRequest {
                queue: "q".into(),
                worker_id: "bench-worker".into(),
                now_ms: clock.tick(),
                lease_ms: 1,
                limit: 0,
                partition_key: None,
            };
            setup.claim_jobs(&request).expect("T8 maintenance");
            let n = uncertain.len();
            uncertain = short_leased
                .iter()
                .copied()
                .filter(|id| {
                    setup
                        .job(*id)
                        .expect("T8 job lookup")
                        .expect("T8 job")
                        .state
                        == minisqlite::JobState::Uncertain
                })
                .collect();
            if uncertain.len() == n {
                break;
            }
        }
        uncertain.truncate(total);
    }
    let store = open_store(&path, durability);
    let mut next = 0usize;
    run_workload(
        &format!("T8 uncertain resolution [{label}]"),
        WARMUP,
        profile.txn_iters,
        || {
            let job_id = uncertain[next];
            next += 1;
            let batch = CommitBatch::new(ids.next(), clock.tick())
                .resolve_uncertain_job(job_id, Resolution::MarkSucceeded);
            store.commit(&batch).expect("T8 resolve");
        },
    );
    print_db_state(&format!("T6-T8 {label}"), &path);
}

// ---------------------------------------------------------------------------
// §2.2 scale workloads + §2.3 O1/O2/O3/O8/O9 (event history)
// ---------------------------------------------------------------------------

fn scale_event_history(root: &Path, profile: &Profile) {
    println!("\n== §2.2 event-history scale + O1/O2/O3/O8/O9 (grown incrementally) ==");
    let dir = root.join("scale-events");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("events.db");
    let mut ids = IdGen::new(4);
    let mut clock = Clock::new();
    let mut population = 0u64;

    for &target in &profile.event_populations {
        {
            let setup = open_store(&path, Durability::Relaxed);
            populate_events(&setup, &mut ids, &mut clock, target - population, 100);
        }
        population = target;

        // T1/T3-style commits at scale, both durability modes.
        for durability in [Durability::Strict, Durability::Relaxed] {
            let label = durability_label(durability);
            let store = open_store(&path, durability);
            let mut n = store.projection_version("scale").expect("scale version");
            run_workload(
                &format!("T3 @ {population} events [{label}]"),
                WARMUP,
                profile.txn_iters,
                || {
                    let job = JobSpec::reconcilable(
                        ids.next(),
                        "scaleq",
                        format!("p{n}"),
                        b"{}".to_vec(),
                    );
                    let batch = CommitBatch::new(ids.next(), clock.tick())
                        .append_event(event(&mut ids, &mut clock, "s0"))
                        .apply_projection_patch(
                            ProjectionPatch::new("scale", n).put(b"key", b"value"),
                        )
                        .enqueue_job(job);
                    store.commit(&batch).expect("scale T3 commit");
                    n += 1;
                },
            );
            population += (WARMUP + profile.txn_iters) as u64;
            // Capture DB/WAL sizes while this store is still open: dropping
            // the store checkpoints and deletes the WAL file.
            if durability == Durability::Relaxed {
                print_db_state(&format!("{population} events"), &path);
            }
        }

        // O1: open time (store dropped and reopened per measurement).
        let mut samples = Vec::new();
        for _ in 0..5 {
            if profile.cold {
                drop_caches();
            }
            let t = Instant::now();
            let store = ControlPlaneStore::open(&path).expect("O1 open");
            store.stream_version("s0").expect("O1 first read");
            samples.push(t.elapsed());
        }
        report(
            &format!("O1 open + first read @ {population} events"),
            &mut samples,
        );
        print_fresh_process_rss(
            root,
            &format!("O1 open @ {population} events"),
            "o1-open",
            Some(&path),
        );

        let store = open_store(&path, Durability::Relaxed);

        // O2: read 100 events from one stream.
        if profile.cold {
            drop_caches();
        }
        run_workload(
            &format!("O2 stream read 100 @ {population} events"),
            WARMUP,
            profile.txn_iters,
            || {
                let events = store.stream_events("s0", 1, 100).expect("O2 stream read");
                assert_eq!(events.len(), 100, "O2 expected 100 events");
            },
        );
        print_fresh_process_rss(
            root,
            &format!("O2 stream read @ {population} events"),
            "o2-read",
            Some(&path),
        );

        // O3: global pagination over the full history in 256-event pages.
        if profile.cold {
            drop_caches();
        }
        let mut pages = Vec::new();
        let mut cursor = 0u64;
        loop {
            let t = Instant::now();
            let events = store.events_after(cursor, PAGE).expect("O3 page");
            pages.push(t.elapsed());
            match events.last() {
                Some(last) if events.len() == PAGE => cursor = last.global_sequence,
                _ => break,
            }
        }
        let (first, last) = (pages[0], pages[pages.len() - 1]);
        report(
            &format!("O3 global pagination page @ {population} events"),
            &mut pages,
        );
        println!(
            "    O3 linearity check: first page {} vs last page {}",
            fmt_dur(first),
            fmt_dur(last)
        );
        print_fresh_process_rss(
            root,
            &format!("O3 pagination @ {population} events"),
            "o3-pages",
            Some(&path),
        );

        // O8: integrity verification.
        if profile.cold {
            drop_caches();
        }
        let t = Instant::now();
        let verify = store.verify().expect("O8 verify");
        report_single(
            &format!("O8 verify @ {population} events"),
            t.elapsed(),
            &format!("ok={}", verify.is_ok()),
        );
        print_fresh_process_rss(
            root,
            &format!("O8 verify @ {population} events"),
            "o8-verify",
            Some(&path),
        );

        // O9: paged diagnostic export of the full history.
        if profile.cold {
            drop_caches();
        }
        let t = Instant::now();
        let export = store.diagnostic_export().expect("O9 export");
        report_single(
            &format!("O9 diagnostic export @ {population} events"),
            t.elapsed(),
            &format!("bytes={}", export.len()),
        );
        print_fresh_process_rss(
            root,
            &format!("O9 export @ {population} events"),
            "o9-export",
            Some(&path),
        );
    }
}

// ---------------------------------------------------------------------------
// §2.2 terminal-job scale + O6 (job pagination)
// ---------------------------------------------------------------------------

fn scale_terminal_jobs(root: &Path, profile: &Profile) {
    println!("\n== §2.2 terminal-job scale + O6 (grown incrementally) ==");
    let dir = root.join("scale-jobs");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("jobs.db");
    let mut ids = IdGen::new(5);
    let mut clock = Clock::new();
    let mut population = 0u64;

    for &target in &profile.terminal_job_populations {
        {
            let setup = open_store(&path, Durability::Relaxed);
            populate_jobs(
                &setup,
                &mut ids,
                &mut clock,
                "histq",
                "h",
                target - population,
                1000,
            );
            terminalize_queue(&setup, &mut ids, &mut clock, "histq");
        }
        population = target;

        let store = open_store(&path, Durability::Relaxed);

        // T4 against a fresh active queue sharing the DB with the terminal history.
        let total = (WARMUP + profile.txn_iters) as u64;
        let live_queue = format!("liveq{population}");
        populate_jobs(&store, &mut ids, &mut clock, &live_queue, "a", total, total);
        run_workload(
            &format!("T4 claim one @ {population} terminal jobs [relaxed]"),
            WARMUP,
            profile.txn_iters,
            || {
                let jobs = claim(&store, &live_queue, clock.tick(), 1);
                assert_eq!(jobs.len(), 1, "scale T4 expected one job");
            },
        );

        // O6: page through the whole terminal history with a moving cursor
        // (like O3/O5), never refetching the first page.
        if profile.cold {
            drop_caches();
        }
        let mut pages = Vec::new();
        let mut cursor = 0u64;
        loop {
            let t = Instant::now();
            let (jobs, next) = store
                .jobs_page(Some("histq"), None, cursor, PAGE)
                .expect("O6 jobs page");
            pages.push(t.elapsed());
            if jobs.len() < PAGE {
                break;
            }
            cursor = next;
        }
        let (first, last) = (pages[0], pages[pages.len() - 1]);
        report(
            &format!("O6 jobs pagination page @ {population} terminal jobs"),
            &mut pages,
        );
        println!(
            "    O6 linearity check: first page {} vs last page {}",
            fmt_dur(first),
            fmt_dur(last)
        );
        print_fresh_process_rss(
            root,
            &format!("O6 jobs pagination @ {population} terminal jobs"),
            "o6-jobs",
            Some(&path),
        );

        print_db_state(&format!("{population} terminal jobs"), &path);
    }
}

// ---------------------------------------------------------------------------
// §2.2 active vs. historical partition scaling (the P1 claim-latency check)
// ---------------------------------------------------------------------------

fn scale_partitions(root: &Path, profile: &Profile) {
    println!("\n== §2.2 partition scaling: claim latency vs. historical partitions ==");
    let dir = root.join("scale-partitions");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut ids = IdGen::new(6);
    let mut clock = Clock::new();

    for &(active, historical) in &profile.partition_mixes {
        let path = dir.join(format!("mix-{active}-{historical}.db"));
        let depth = (WARMUP + profile.claim_iters) as u64;
        {
            let setup = open_store(&path, Durability::Relaxed);
            populate_jobs(
                &setup, &mut ids, &mut clock, "q", "hist", historical, historical,
            );
            terminalize_queue(&setup, &mut ids, &mut clock, "q");
            populate_jobs(
                &setup,
                &mut ids,
                &mut clock,
                "q",
                "act",
                active * depth,
                active,
            );
        }
        let store = open_store(&path, Durability::Relaxed);
        let limit = 100.min(active as usize);
        let mut state = (&mut ids, &mut clock, Vec::<ClaimedJob>::new());
        run_workload_prepped(
            &format!("T5 claim {limit} @ {active} active / {historical} historical"),
            WARMUP,
            profile.claim_iters,
            &mut state,
            |s| {
                ack_all(&store, s.0, s.1, &s.2);
                s.2.clear();
            },
            |s| {
                s.2 = claim(&store, "q", s.1.tick(), limit);
                assert_eq!(s.2.len(), limit, "partition-mix claim shortfall");
            },
        );
        print_db_state(
            &format!("{active} active / {historical} historical partitions"),
            &path,
        );
    }
    println!("    assertion: claim latency must not grow with historical partitions (§2.2)");
}

// ---------------------------------------------------------------------------
// §2.3 O4/O5: projection point read and prefix scan
// ---------------------------------------------------------------------------

fn operational_projections(root: &Path, profile: &Profile) {
    println!("\n== §2.3 projection reads (O4/O5, 10k-entry projection) ==");
    let dir = root.join("projections");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("proj.db");
    let mut ids = IdGen::new(7);
    let mut clock = Clock::new();
    let entries = 10_000u64;
    {
        let setup = open_store(&path, Durability::Relaxed);
        let mut version = 0u64;
        let mut written = 0u64;
        while written < entries {
            let n = POPULATE_BATCH.min((entries - written) as usize);
            let mut patch = ProjectionPatch::new("proj", version);
            for i in 0..n {
                let key = format!("key/{:08}", written + i as u64);
                patch = patch.put(key.into_bytes(), b"value".to_vec());
            }
            let batch = CommitBatch::new(ids.next(), clock.tick()).apply_projection_patch(patch);
            setup.commit(&batch).expect("populate projection");
            version += 1;
            written += n as u64;
        }
    }
    let store = open_store(&path, Durability::Relaxed);

    if profile.cold {
        drop_caches();
    }
    let mut n = 0u64;
    run_workload(
        "O4 projection point read",
        WARMUP,
        profile.txn_iters,
        || {
            let key = format!("key/{:08}", (n * 7919) % entries);
            let value = store
                .projection_get("proj", key.as_bytes())
                .expect("O4 get");
            assert!(value.is_some(), "O4 expected a value");
            n += 1;
        },
    );
    print_fresh_process_rss(root, "O4 projection point read", "o4-point", Some(&path));

    if profile.cold {
        drop_caches();
    }
    let mut pages = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let t = Instant::now();
        let page = store
            .projection_scan_prefix_page("proj", b"key/", after.as_deref(), PAGE)
            .expect("O5 page");
        pages.push(t.elapsed());
        match page.last() {
            Some(last) if page.len() == PAGE => after = Some(last.key.clone()),
            _ => break,
        }
    }
    report("O5 prefix scan page (256 entries)", &mut pages);
    print_fresh_process_rss(root, "O5 prefix scan", "o5-scan", Some(&path));
    print_db_state("O4/O5 projection fixture", &path);
}

// ---------------------------------------------------------------------------
// §2.3 O7: live backup writer stall
// ---------------------------------------------------------------------------

fn live_backup(root: &Path, profile: &Profile) {
    println!("\n== §2.3 O7 live backup (writer mutex serialization while commits continue) ==");
    let dir = root.join("backup");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("source.db");
    let mut ids = IdGen::new(8);
    let mut clock = Clock::new();

    // Grow the source DB toward the target size with 32 KiB event payloads.
    let payload = vec![b'x'; 32 << 10];
    {
        let setup = open_store(&path, Durability::Relaxed);
        loop {
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            if size >= profile.backup_target_bytes {
                break;
            }
            let mut batch = CommitBatch::new(ids.next(), clock.tick());
            for _ in 0..256 {
                batch = batch.append_event(Event::with_json_payload(
                    ids.next(),
                    "bulk",
                    "bench",
                    clock.tick(),
                    &payload,
                ));
            }
            setup.commit(&batch).expect("grow backup source");
        }
    }
    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    let store = open_store(&path, Durability::Relaxed);
    let stop = AtomicBool::new(false);
    let dest = dir.join("backup.db");
    let (backup_time, max_stall) = std::thread::scope(|scope| {
        let store = &store;
        let stop = &stop;
        let writer = scope.spawn(move || {
            let mut ids = IdGen::new(9);
            let mut now = 10_000_000i64;
            let mut max_commit = Duration::ZERO;
            while !stop.load(Ordering::Relaxed) {
                now += 1;
                let batch = CommitBatch::new(ids.next(), now).append_event(
                    Event::with_json_payload(ids.next(), "live", "bench", now, b"{}"),
                );
                let t = Instant::now();
                store.commit(&batch).expect("O7 live commit");
                max_commit = max_commit.max(t.elapsed());
            }
            max_commit
        });
        std::thread::sleep(Duration::from_millis(50));
        let t = Instant::now();
        store.backup(&dest, true).expect("O7 backup");
        let backup_time = t.elapsed();
        stop.store(true, Ordering::Relaxed);
        (backup_time, writer.join().expect("O7 writer thread"))
    });
    report_single(
        &format!("O7 live backup of {} MiB DB", size >> 20),
        backup_time,
        &format!(
            "writer mutex serialization: worst commit latency behind backup={}",
            fmt_dur(max_stall)
        ),
    );
    print_db_state("O7 backup source", &path);
    print_fresh_process_rss(root, "O7 backup", "o7-backup", Some(&path));
}
