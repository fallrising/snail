//! Benchmark / connection-hold client for rudis.
//!
//! Modes:
//!   (default) latency: GET/SET 8:2, no pipeline
//!   --hold: open N connections, keep alive with PING, report hold success
//!
//! Latency gate: errors=0 and p99 < --p99-ms (default 5). Use --soft to report
//! FAIL without exiting non-zero (full-active stress / aspirational profiles).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Barrier;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Latency,
    Hold,
}

#[derive(Debug, Clone)]
struct Config {
    host: String,
    port: u16,
    clients: usize,
    requests_per_client: usize,
    warmup_per_client: usize,
    get_ratio: f64,
    key_count: usize,
    seed_keys: bool,
    connect_batch: usize,
    mode: Mode,
    hold_secs: u64,
    ping_interval_ms: u64,
    /// Spread hold clients across 127.0.0.1 .. 127.0.0.N (needs server bind 0.0.0.0).
    loopback_spread: u8,
    /// p99 gate in milliseconds (latency mode).
    p99_target_ms: f64,
    /// If true, latency p99 miss prints FAIL but exits 0.
    soft: bool,
    /// Of `-c` connections, only this many issue GET/SET (0 = all). Others stay idle
    /// so we can measure mid-concurrency latency while holding C10K fds.
    active: usize,
    /// Commands in flight per active connection (1 = request/response).
    pipeline: usize,
}

impl Config {
    fn from_args() -> Self {
        let mut cfg = Self {
            host: "127.0.0.1".into(),
            port: 6379,
            clients: 10_000,
            requests_per_client: 100,
            warmup_per_client: 10,
            get_ratio: 0.8,
            key_count: 10_000,
            seed_keys: true,
            connect_batch: 500,
            mode: Mode::Latency,
            hold_secs: 30,
            ping_interval_ms: 1000,
            loopback_spread: 1,
            p99_target_ms: 5.0,
            soft: false,
            active: 0,
            pipeline: 1,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--host" => cfg.host = args.next().expect("--host"),
                "--port" => cfg.port = args.next().expect("--port").parse().expect("port"),
                "-c" | "--clients" => {
                    cfg.clients = args.next().expect("clients").parse().expect("clients")
                }
                "-n" | "--requests" => {
                    cfg.requests_per_client =
                        args.next().expect("requests").parse().expect("requests")
                }
                "--warmup" => {
                    cfg.warmup_per_client =
                        args.next().expect("warmup").parse().expect("warmup")
                }
                "--get-ratio" => {
                    cfg.get_ratio = args.next().expect("ratio").parse().expect("ratio")
                }
                "--keys" => cfg.key_count = args.next().expect("keys").parse().expect("keys"),
                "--no-seed" => cfg.seed_keys = false,
                "--connect-batch" => {
                    cfg.connect_batch = args.next().expect("batch").parse().expect("batch")
                }
                "--hold" => {
                    cfg.mode = Mode::Hold;
                    cfg.seed_keys = false;
                }
                "--hold-secs" => {
                    cfg.hold_secs = args.next().expect("hold-secs").parse().expect("secs")
                }
                "--ping-interval-ms" => {
                    cfg.ping_interval_ms = args
                        .next()
                        .expect("ping-interval")
                        .parse()
                        .expect("ms")
                }
                "--loopback-spread" => {
                    cfg.loopback_spread = args
                        .next()
                        .expect("spread")
                        .parse()
                        .expect("u8")
                }
                "--p99-ms" => {
                    cfg.p99_target_ms = args.next().expect("p99-ms").parse().expect("f64")
                }
                "--soft" => cfg.soft = true,
                "--active" => {
                    cfg.active = args.next().expect("active").parse().expect("active")
                }
                "--pipeline" | "-P" => {
                    cfg.pipeline = args
                        .next()
                        .expect("pipeline")
                        .parse::<usize>()
                        .expect("pipeline")
                        .max(1)
                }
                "--help" | "-h" => {
                    eprintln!(
                        "rudis-bench [--hold] [-c N] [--active K] [-n M] [-P pipeline] [--port P] [--p99-ms 5] [--soft]\n  --active K: only K of N connections issue load (hold rest idle)\n  -P N: pipeline depth per connection (throughput)\n  --soft: latency miss exits 0"
                    );
                    std::process::exit(0);
                }
                other => {
                    eprintln!("unknown arg: {other}");
                    std::process::exit(1);
                }
            }
        }
        cfg
    }
}

fn main() {
    let cfg = Config::from_args();
    let threads = std::thread::available_parallelism()
        .map(|n| n.get().max(4))
        .unwrap_or(8);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async move {
        match cfg.mode {
            Mode::Latency => run_latency(cfg).await,
            Mode::Hold => run_hold(cfg).await,
        }
    });
}

async fn run_latency(cfg: Config) {
    let active = if cfg.active == 0 {
        cfg.clients
    } else {
        cfg.active.min(cfg.clients)
    };
    eprintln!(
        "rudis-bench latency: {}:{} clients={} active={} measure={} warmup={} get_ratio={} pipeline={}",
        cfg.host,
        cfg.port,
        cfg.clients,
        active,
        cfg.requests_per_client,
        cfg.warmup_per_client,
        cfg.get_ratio,
        cfg.pipeline
    );

    if cfg.seed_keys {
        eprintln!("seeding {} keys...", cfg.key_count);
        seed_keys(&cfg).await;
    }

    let errors = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::<u64>::new()));
    let connect_barrier = Arc::new(Barrier::new(cfg.clients + 1));
    let finish_barrier = Arc::new(Barrier::new(cfg.clients));

    let mut handles = Vec::new();
    for batch_start in (0..cfg.clients).step_by(cfg.connect_batch) {
        let batch_end = (batch_start + cfg.connect_batch).min(cfg.clients);
        for id in batch_start..batch_end {
            let cfg = cfg.clone();
            let errors = errors.clone();
            let latencies = latencies.clone();
            let connect_barrier = connect_barrier.clone();
            let finish_barrier = finish_barrier.clone();
            handles.push(tokio::spawn(async move {
                run_client(
                    id,
                    active,
                    cfg,
                    errors,
                    latencies,
                    connect_barrier,
                    finish_barrier,
                )
                .await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    connect_barrier.wait().await;
    eprintln!(
        "all {} clients connected ({} active), measuring...",
        cfg.clients, active
    );

    let start = Instant::now();
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();

    let mut lats = latencies.lock().await.clone();
    lats.sort_unstable();

    let total = active * cfg.requests_per_client;
    let ok = lats.len();
    let err = errors.load(Ordering::Relaxed);

    print_latency_report(&cfg, active, total, ok, err, elapsed.as_secs_f64(), &lats);
}

async fn run_hold(cfg: Config) {
    let spread = cfg.loopback_spread.max(1);
    eprintln!(
        "rudis-bench hold: port={} clients={} hold={}s ping_every={}ms loopback_spread={}",
        cfg.port, cfg.clients, cfg.hold_secs, cfg.ping_interval_ms, spread
    );
    if spread > 1 {
        eprintln!(
            "note: connecting via 127.0.0.1..127.0.0.{} (server must bind 0.0.0.0)",
            spread
        );
    }

    let connected = Arc::new(AtomicUsize::new(0));
    let connect_fail = Arc::new(AtomicUsize::new(0));
    let ping_ok = Arc::new(AtomicUsize::new(0));
    let ping_err = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::<u64>::new()));
    let connect_barrier = Arc::new(Barrier::new(cfg.clients + 1));
    // Hold timer starts after all clients are connected.
    let hold_secs = cfg.hold_secs;

    let mut handles = Vec::new();
    for batch_start in (0..cfg.clients).step_by(cfg.connect_batch) {
        let batch_end = (batch_start + cfg.connect_batch).min(cfg.clients);
        for id in batch_start..batch_end {
            let cfg = cfg.clone();
            let connected = connected.clone();
            let connect_fail = connect_fail.clone();
            let ping_ok = ping_ok.clone();
            let ping_err = ping_err.clone();
            let latencies = latencies.clone();
            let connect_barrier = connect_barrier.clone();
            handles.push(tokio::spawn(async move {
                let host = if cfg.loopback_spread > 1 {
                    let octet = 1 + (id % cfg.loopback_spread as usize);
                    format!("127.0.0.{octet}")
                } else {
                    cfg.host.clone()
                };
                let addr = format!("{}:{}", host, cfg.port);
                let mut stream = match connect_with_retry(&addr, 5).await {
                    Some(s) => {
                        let n = connected.fetch_add(1, Ordering::Relaxed) + 1;
                        if n % 10000 == 0 {
                            eprintln!("connect progress: {n}");
                        }
                        s
                    }
                    None => {
                        connect_fail.fetch_add(1, Ordering::Relaxed);
                        connect_barrier.wait().await;
                        return;
                    }
                };
                connect_barrier.wait().await;

                let deadline = Instant::now() + Duration::from_secs(hold_secs);
                let ping = resp_cmd(&["PING"]);
                let mut local = Vec::new();
                while Instant::now() < deadline {
                    match exchange(&mut stream, &ping).await {
                        Ok(us) => {
                            ping_ok.fetch_add(1, Ordering::Relaxed);
                            local.push(us);
                        }
                        Err(_) => {
                            ping_err.fetch_add(1, Ordering::Relaxed);
                            if let Some(s) = connect_with_retry(&addr, 2).await {
                                stream = s;
                            } else {
                                break;
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(cfg.ping_interval_ms.max(1))).await;
                }
                latencies.lock().await.extend(local);
            }));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    connect_barrier.wait().await;
    let live = connected.load(Ordering::Relaxed);
    eprintln!(
        "hold: connected={live} failed={} — holding {}s...",
        connect_fail.load(Ordering::Relaxed),
        hold_secs
    );

    for h in handles {
        let _ = h.await;
    }

    let mut lats = latencies.lock().await.clone();
    lats.sort_unstable();
    let ok = ping_ok.load(Ordering::Relaxed);
    let err = ping_err.load(Ordering::Relaxed);
    let fail = connect_fail.load(Ordering::Relaxed);

    println!();
    println!("====== rudis connection hold ======");
    println!("target clients:  {}", cfg.clients);
    println!("connected:       {live}");
    println!("connect failed:  {fail}");
    println!("loopback spread: {spread}");
    println!("hold duration:   {}s", hold_secs);
    println!("ping ok:         {ok}");
    println!("ping errors:     {err}");
    if !lats.is_empty() {
        println!(
            "ping p50/p99:    {} / {} µs",
            percentile(&lats, 50.0),
            percentile(&lats, 99.0)
        );
    }

    let conn_ok = live * 100 >= cfg.clients * 99;
    let ping_total = ok + err;
    let ping_ok_ratio = ping_total == 0 || err * 100 < ping_total;
    if conn_ok && ping_ok_ratio && fail == 0 {
        println!();
        println!("RESULT: PASS (connected={live}/{}, ping_err={err})", cfg.clients);
    } else {
        println!();
        println!(
            "RESULT: FAIL (connected={live}/{}, connect_fail={fail}, ping_err={err})",
            cfg.clients
        );
        std::process::exit(1);
    }
}

fn print_latency_report(
    cfg: &Config,
    active: usize,
    total: usize,
    ok: usize,
    err: usize,
    secs: f64,
    lats: &[u64],
) {
    println!();
    println!("====== rudis C10K benchmark ======");
    println!("connections:     {}", cfg.clients);
    println!("active load:     {active}");
    println!("warmup/conn:     {}", cfg.warmup_per_client);
    println!("measured/active: {}", cfg.requests_per_client);
    println!("total measured:  {total}");
    println!("successful:      {ok}");
    println!("errors:          {err}");
    println!("duration:        {secs:.2}s");
    println!("throughput:      {:.0} req/s", ok as f64 / secs.max(1e-9));
    println!("get/set ratio:   {:.0}% GET", cfg.get_ratio * 100.0);
    println!("pipeline:        {}", cfg.pipeline);

    if lats.is_empty() {
        println!("RESULT: FAIL (no successful requests)");
        std::process::exit(1);
    }

    let avg: f64 = lats.iter().map(|&x| x as f64).sum::<f64>() / ok as f64;
    let p50 = percentile(lats, 50.0);
    let p99 = percentile(lats, 99.0);
    let p999 = percentile(lats, 99.9);

    println!();
    println!("latency (µs):");
    println!("  avg:  {avg:.0}");
    println!("  p50:  {p50}");
    println!("  p99:  {p99}");
    println!("  p999: {p999}");

    let p99_ms = p99 as f64 / 1000.0;
    let target = cfg.p99_target_ms;
    if err == 0 && p99_ms < target {
        println!();
        println!("RESULT: PASS (errors=0, p99={p99_ms:.2}ms < {target}ms)");
    } else {
        println!();
        println!(
            "RESULT: FAIL (errors={err}, p99={p99_ms:.2}ms, target p99<{target}ms errors=0)"
        );
        if !cfg.soft {
            std::process::exit(1);
        }
    }
}

async fn run_client(
    id: usize,
    active: usize,
    cfg: Config,
    errors: Arc<AtomicUsize>,
    latencies: Arc<tokio::sync::Mutex<Vec<u64>>>,
    connect_barrier: Arc<Barrier>,
    finish_barrier: Arc<Barrier>,
) {
    let addr = format!("{}:{}", cfg.host, cfg.port);
    let mut stream = match connect_with_retry(&addr, 5).await {
        Some(s) => s,
        None => {
            if id < active {
                errors.fetch_add(
                    cfg.warmup_per_client + cfg.requests_per_client,
                    Ordering::Relaxed,
                );
            }
            connect_barrier.wait().await;
            finish_barrier.wait().await;
            return;
        }
    };

    connect_barrier.wait().await;

    if id < active {
        let mut rng = StdRng::seed_from_u64(id as u64 + 0x9E37_79B9_7F4A_7C15);
        let mut local = Vec::with_capacity(cfg.requests_per_client);
        let pipe = cfg.pipeline.max(1);

        let mut remaining_warmup = cfg.warmup_per_client;
        while remaining_warmup > 0 {
            let batch = remaining_warmup.min(pipe);
            if !pipeline_batch(&mut stream, &addr, &mut rng, &cfg, &errors, None, batch).await
            {
                finish_barrier.wait().await;
                return;
            }
            remaining_warmup -= batch;
        }

        let mut remaining = cfg.requests_per_client;
        while remaining > 0 {
            let batch = remaining.min(pipe);
            if !pipeline_batch(
                &mut stream,
                &addr,
                &mut rng,
                &cfg,
                &errors,
                Some(&mut local),
                batch,
            )
            .await
            {
                finish_barrier.wait().await;
                return;
            }
            remaining -= batch;
        }

        latencies.lock().await.extend(local);
    }

    // Keep idle sockets open until active clients finish measuring.
    finish_barrier.wait().await;
    let _ = stream;
}

async fn pipeline_batch(
    stream: &mut TcpStream,
    addr: &str,
    rng: &mut StdRng,
    cfg: &Config,
    errors: &Arc<AtomicUsize>,
    mut record: Option<&mut Vec<u64>>,
    batch: usize,
) -> bool {
    let mut cmds = String::with_capacity(batch * 48);
    for _ in 0..batch {
        let kid = rng.gen_range(0..cfg.key_count);
        let key = format!("bk{kid}");
        if rng.gen::<f64>() < cfg.get_ratio {
            cmds.push_str(&resp_cmd(&["GET", &key]));
        } else {
            cmds.push_str(&resp_cmd(&["SET", &key, "v"]));
        }
    }

    let t0 = Instant::now();
    if stream.write_all(cmds.as_bytes()).await.is_err() {
        errors.fetch_add(batch, Ordering::Relaxed);
        return reconnect(stream, addr).await;
    }

    let mut buf = vec![0u8; 4096];
    let mut filled = 0usize;
    let mut parsed = 0usize;
    while parsed < batch {
        match stream.read(&mut buf[filled..]).await {
            Ok(0) => {
                errors.fetch_add(batch - parsed, Ordering::Relaxed);
                return reconnect(stream, addr).await;
            }
            Ok(n) => {
                filled += n;
                while parsed < batch {
                    match try_consume_resp(&buf[..filled]) {
                        Some(used) => {
                            buf.copy_within(used..filled, 0);
                            filled -= used;
                            parsed += 1;
                            if let Some(ref mut lat) = record {
                                lat.push(t0.elapsed().as_micros() as u64);
                            }
                        }
                        None => break,
                    }
                }
                if filled == buf.len() {
                    buf.resize(buf.len() * 2, 0);
                }
            }
            Err(_) => {
                errors.fetch_add(batch - parsed, Ordering::Relaxed);
                return reconnect(stream, addr).await;
            }
        }
    }
    true
}

async fn reconnect(stream: &mut TcpStream, addr: &str) -> bool {
    match connect_with_retry(addr, 3).await {
        Some(s) => {
            *stream = s;
            true
        }
        None => false,
    }
}

/// Return bytes consumed by one complete RESP value, or None if incomplete.
fn try_consume_resp(buf: &[u8]) -> Option<usize> {
    if buf.is_empty() {
        return None;
    }
    match buf[0] {
        b'+' | b'-' | b':' => {
            let end = find_crlf(buf)?;
            Some(end + 2)
        }
        b'$' => {
            let hdr = find_crlf(buf)?;
            let len: isize = std::str::from_utf8(&buf[1..hdr]).ok()?.parse().ok()?;
            if len < 0 {
                return Some(hdr + 2);
            }
            let start = hdr + 2;
            let total = start + len as usize + 2;
            if buf.len() < total {
                return None;
            }
            Some(total)
        }
        b'*' => {
            let hdr = find_crlf(buf)?;
            let n: isize = std::str::from_utf8(&buf[1..hdr]).ok()?.parse().ok()?;
            if n < 0 {
                return Some(hdr + 2);
            }
            let mut off = hdr + 2;
            for _ in 0..n {
                let used = try_consume_resp(&buf[off..])?;
                off += used;
            }
            Some(off)
        }
        _ => Some(1), // resync skip
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

async fn connect_with_retry(addr: &str, attempts: usize) -> Option<TcpStream> {
    for i in 0..attempts {
        match TcpStream::connect(addr).await {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                return Some(s);
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(10 * (i as u64 + 1))).await,
        }
    }
    None
}

async fn seed_keys(cfg: &Config) {
    let addr = format!("{}:{}", cfg.host, cfg.port);
    let chunk = 256;
    let mut handles = Vec::new();
    for start in (0..cfg.key_count).step_by(chunk) {
        let end = (start + chunk).min(cfg.key_count);
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut stream = connect_with_retry(&addr, 5)
                .await
                .expect("seed connect");
            for i in start..end {
                let key = format!("bk{i}");
                let cmd = resp_cmd(&["SET", &key, "warmup"]);
                let _ = exchange(&mut stream, &cmd).await;
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

async fn exchange(stream: &mut TcpStream, cmd: &str) -> Result<u64, ()> {
    let t0 = Instant::now();
    stream.write_all(cmd.as_bytes()).await.map_err(|_| ())?;
    let mut buf = [0u8; 512];
    let n = stream.read(&mut buf).await.map_err(|_| ())?;
    if n == 0 {
        return Err(());
    }
    Ok(t0.elapsed().as_micros() as u64)
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    let idx = ((sorted.len() as f64 - 1.0) * pct / 100.0).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn resp_cmd(args: &[&str]) -> String {
    let mut s = format!("*{}\r\n", args.len());
    for a in args {
        s.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    s
}
