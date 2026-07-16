//! C10K benchmark client — async tokio, GET/SET 8:2, no pipeline.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Barrier;

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
                "--help" | "-h" => {
                    eprintln!("rudis-bench: see scripts/bench-c10k.sh");
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

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let cfg = Config::from_args();
    eprintln!(
        "rudis-bench: {}:{} clients={} measure={} warmup={} get_ratio={}",
        cfg.host,
        cfg.port,
        cfg.clients,
        cfg.requests_per_client,
        cfg.warmup_per_client,
        cfg.get_ratio
    );

    if cfg.seed_keys {
        eprintln!("seeding {} keys...", cfg.key_count);
        seed_keys(&cfg).await;
    }

    let errors = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::<u64>::new()));
    let connect_barrier = Arc::new(Barrier::new(cfg.clients + 1));

    let mut handles = Vec::new();
    for batch_start in (0..cfg.clients).step_by(cfg.connect_batch) {
        let batch_end = (batch_start + cfg.connect_batch).min(cfg.clients);
        for id in batch_start..batch_end {
            let cfg = cfg.clone();
            let errors = errors.clone();
            let latencies = latencies.clone();
            let connect_barrier = connect_barrier.clone();
            handles.push(tokio::spawn(async move {
                run_client(id, cfg, errors, latencies, connect_barrier).await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    connect_barrier.wait().await;
    eprintln!("all {} clients connected, measuring...", cfg.clients);

    let start = Instant::now();
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();

    let mut lats = latencies.lock().await.clone();
    lats.sort_unstable();

    let total = cfg.clients * cfg.requests_per_client;
    let ok = lats.len();
    let err = errors.load(Ordering::Relaxed);

    print_report(&cfg, total, ok, err, elapsed.as_secs_f64(), &lats);
}

fn print_report(cfg: &Config, total: usize, ok: usize, err: usize, secs: f64, lats: &[u64]) {
    println!();
    println!("====== rudis C10K benchmark ======");
    println!("connections:     {}", cfg.clients);
    println!("warmup/conn:     {}", cfg.warmup_per_client);
    println!("measured/conn:   {}", cfg.requests_per_client);
    println!("total measured:  {total}");
    println!("successful:      {ok}");
    println!("errors:          {err}");
    println!("duration:        {secs:.2}s");
    println!("throughput:      {:.0} req/s", ok as f64 / secs);
    println!("get/set ratio:   {:.0}% GET", cfg.get_ratio * 100.0);

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
    if err == 0 && p99_ms < 5.0 {
        println!();
        println!("RESULT: PASS (errors=0, p99={p99_ms:.2}ms < 5ms)");
    } else {
        println!();
        println!("RESULT: FAIL (errors={err}, p99={p99_ms:.2}ms, target p99<5ms errors=0)");
        std::process::exit(1);
    }
}

async fn run_client(
    id: usize,
    cfg: Config,
    errors: Arc<AtomicUsize>,
    latencies: Arc<tokio::sync::Mutex<Vec<u64>>>,
    connect_barrier: Arc<Barrier>,
) {
    let addr = format!("{}:{}", cfg.host, cfg.port);
    let mut stream = match connect_with_retry(&addr, 5).await {
        Some(s) => s,
        None => {
            errors.fetch_add(
                cfg.warmup_per_client + cfg.requests_per_client,
                Ordering::Relaxed,
            );
            connect_barrier.wait().await;
            return;
        }
    };

    connect_barrier.wait().await;

    let mut rng = StdRng::seed_from_u64(id as u64 + 0x9E37_79B9_7F4A_7C15);
    let mut local = Vec::with_capacity(cfg.requests_per_client);

    for _ in 0..cfg.warmup_per_client {
        if !one_request(&mut stream, &addr, &mut rng, &cfg, &errors, None).await {
            return;
        }
    }

    for _ in 0..cfg.requests_per_client {
        if !one_request(&mut stream, &addr, &mut rng, &cfg, &errors, Some(&mut local)).await {
            return;
        }
    }

    latencies.lock().await.extend(local);
}

async fn one_request(
    stream: &mut TcpStream,
    addr: &str,
    rng: &mut StdRng,
    cfg: &Config,
    errors: &Arc<AtomicUsize>,
    record: Option<&mut Vec<u64>>,
) -> bool {
    let key = format!("bk{}", rng.gen_range(0..cfg.key_count));
    let cmd = if rng.gen::<f64>() < cfg.get_ratio {
        resp_cmd(&["GET", &key])
    } else {
        resp_cmd(&["SET", &key, "v"])
    };

    match exchange(stream, &cmd).await {
        Ok(us) => {
            if let Some(lat) = record {
                lat.push(us);
            }
            true
        }
        Err(_) => {
            errors.fetch_add(1, Ordering::Relaxed);
            match connect_with_retry(addr, 3).await {
                Some(s) => {
                    *stream = s;
                    true
                }
                None => false,
            }
        }
    }
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