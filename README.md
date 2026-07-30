# rudis

Rust in-memory **Redis-compatible** server speaking **RESP2 over TCP**.

`redis-cli`、`redis-benchmark` 及主流 Redis SDK 可直接連線，零適配成本。

## Status

| Milestone | State |
|---|---|
| M0 Skeleton | Done |
| M1 Full semantics + multi-core | Done (C10K gate PASS, 1w & multi-w) |
| M2 Pressure layer (C100K) | In progress (mio + C100K hold PASS) |
| M3 Limits (C1M / io_uring) | In progress (`RUDIS_IO_URING=1`: AcceptMulti + eventfd + always-in-flight I/O; `scripts/bench-c1m.sh`) |

詳細進度、已知限制與下一步計畫見 [docs/STATUS.md](docs/STATUS.md)。  
架構設計見 [docs/design.md](docs/design.md)。

## Quick start

```bash
cargo build --release
./target/release/rudis --port 6379
```

驗證（需 `redis-cli` 或任意 RESP client）：

```bash
redis-cli -p 6379 ping          # +PONG
redis-cli -p 6379 set foo bar   # +OK
redis-cli -p 6379 get foo       # "bar"
```

跑測試：

```bash
cargo test   # 28 tests
```

## Architecture

```
Clients (RESP2 / pipeline)
    ↓  TCP
Kernel SO_REUSEPORT → Worker × N (pin to core)
    ├── mio/epoll Reactor (accept + ready-FD drive)
    ├── Shard apply (folded into reactor, MPSC try_recv)
    ├── Shard × M (dict + expire index)
    └── Expire (folded, 100ms)
```

- **thread-per-core share-nothing**：每 shard 獨占一 worker，熱路徑零鎖
- **跨 shard**：MPSC channel + oneshot scatter-gather，backpressure 逐級傳導
- **Pipeline**：FIFO 保序，本地/遠端命令可並發完成、按序回覆
- **Telemetry**：`INFO STATS` 暴露全局與 per-shard commands / keys / expires / memory

## Supported commands (M1)

| Category | Examples |
|---|---|
| Server | `PING`, `ECHO`, `HELLO`, `INFO`, `DBSIZE`, `FLUSHDB`, `CONFIG GET`, `COMMAND` |
| String | `GET`, `SET`, `INCR`, `MGET`, `MSET`, `APPEND`, `GETRANGE` |
| List | `LPUSH`, `RPOP`, `LRANGE`, `LTRIM`, `LINDEX` |
| Hash | `HSET`, `HGET`, `HGETALL`, `HINCRBY`, `HSCAN` |
| Set | `SADD`, `SINTER`, `SUNION`, `SMEMBERS`, `SPOP`, `SSCAN` |
| ZSet | `ZADD`, `ZRANGE`, `ZRANGEBYSCORE`, `ZRANK`, `ZPOPMIN` |
| Keys | `DEL`, `EXPIRE`, `TTL`, `SCAN`, `TYPE`, `RENAME` |

完整清單與覆蓋缺口見 [docs/STATUS.md](docs/STATUS.md)。

## Configuration

優先序：CLI flags > `RUDIS_*` 環境變數 > TOML 檔 > 內建默認。

```bash
rudis \
  --bind 0.0.0.0 \
  --port 6379 \
  --workers 4 \
  --shards 4 \
  --maxclients 65536 \
  --maxmemory 0
```

| 參數 | 默認 | 說明 |
|---|---|---|
| `workers` | CPU 核數 | thread-per-core |
| `shards` | = workers | 須為 workers 整數倍 |
| `maxclients` | 65536 | 全局連線閘門 |
| `maxmemory` | 0（不限） | 超限寫入回 `-OOM` |
| `max_bulk_len` | 32 MiB | 單 bulk 上限（C1M 安全閥） |

環境變數範例：`RUDIS_PORT=6380 RUDIS_WORKERS=8 ./target/release/rudis`

## Semantic differences from standalone Redis

- **RESP2 only** — `HELLO 3` 回錯
- **Single DB** — `SELECT` 僅接受 `0`
- **No cross-shard atomicity (I5)** — `MSET`/`DEL` 等跨 shard 時逐 shard 執行，無全局原子性（同 Redis Cluster 語義）；同 shard 內原子
- **RENAME cross-shard** — 不同 shard 的 key 回 CROSSSLOT 錯誤
- **SCAN weak semantics** — 桶位游標，rehash 時可能漏鍵
- **Memory stats approximate** — `maxmemory` 觸發點 ±20%
- **No persistence** — 重啟即空
- **noeviction only** — 超限寫入報 `-OOM`

## Benchmarking

```bash
# C10K 延遲驗收
./scripts/bench-c10k.sh all

# C100K 連線持有（需 ulimit -n 足夠；用 loopback spread 避開 ephemeral port 上限）
CLIENTS=100000 LOOPBACK_SPREAD=64 ./scripts/bench-c100k.sh all

# OS 調優（需 root；含還原）
sudo ./scripts/sysctl-tuning.sh apply
sudo ./scripts/sysctl-tuning.sh restore
```

C10K 驗收口徑：
1. **Gate**：hold 10K 連線，其中 ACTIVE（預設 64）做 GET/SET，p99 &lt; 5ms、零錯誤
2. **Stress**（資訊）：10K 全活躍；達 p99&lt;5ms 約需 ~2M req/s

**現況**：
- **C10K gate：PASS**（1 worker p99 ~1.3ms；多 worker p99 ~1.5–2.4ms）
- **C100K hold：PASS**（`LOOPBACK_SPREAD=64`）
- 10K 全活躍 stress：~100K req/s（pipeline=1）；**~800K req/s**（`PIPELINE=16`）
- 達 p99&lt;5ms@10K in-flight 約需 ~2M req/s（吞吐目標）

## Development

```bash
cargo build          # debug
cargo build --release
cargo test           # 28 tests
```

### Project layout

```
snail/
├── src/           # 實作源碼
│   └── bin/       # rudis-bench 壓測客戶端
├── tests/         # 協議 + 整合 + 命令黑盒測試
├── scripts/       # bench、bench-c10k、sysctl 調優
└── docs/
    ├── design.md  # 技術設計
    └── STATUS.md  # 開發狀態與路線圖
```

### Next steps

1. 全活躍吞吐繼續抬升（目標 ~2M req/s）
2. M3：io_uring / C1M hold

## License

MIT
