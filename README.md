# rudis

Rust in-memory **Redis-compatible** server speaking **RESP2 over TCP**.

`redis-cli`、`redis-benchmark` 及主流 Redis SDK 可直接連線，零適配成本。

## Status

| Milestone | State |
|---|---|
| M0 Skeleton | Done |
| M1 Full semantics + multi-core | In progress (~80%) |
| M2 Pressure layer (C100K) | Started (bench tooling ready) |
| M3 Limits (C1M) | Planned |

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
cargo test   # 18 tests
```

## Architecture

```
Clients (RESP2 / pipeline)
    ↓  TCP
Kernel SO_REUSEPORT → Worker × N (pin to core)
    ├── accept loop → Conn Task (read → parse → dispatch → ordered write)
    ├── Shard Executor (MPSC, sync apply)
    ├── Shard × M (dict + expire index, lock-free)
    └── Expire Ticker (100ms)
```

- **thread-per-core share-nothing**：每 shard 獨占一 worker，熱路徑零鎖
- **跨 shard**：MPSC channel + oneshot scatter-gather，backpressure 逐級傳導
- **Pipeline**：FIFO 保序，本地/遠端命令可並發完成、按序回覆

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
# 構建壓測工具
cargo build --release --bin rudis --bin rudis-bench

# C10K 驗收（需 ulimit -n 65536）
ulimit -n 65536
./scripts/bench-c10k.sh all

# 簡易壓測（有 redis-benchmark 時優先使用，否則回退 rudis-bench）
./scripts/bench.sh

# 高連線數 OS 調優（需 root）
sudo ./scripts/sysctl-tuning.sh
```

C10K 驗收口徑：10K 全活躍、GET/SET 8:2、無 pipeline、p99 < 5ms、零錯誤。

**最新結果**（12 workers / 12 shards）：1M 請求、零錯誤、~156K req/s，p99 ≈ 148 ms（未達標）。詳見 [docs/STATUS.md](docs/STATUS.md)。

## Development

```bash
cargo build          # debug
cargo build --release
cargo test           # 18 tests
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

1. 每 worker 連線反應器（C10K p99 達標關鍵）
2. per-shard ops 指標 + 負載均衡驗證
3. M2 承壓層（buffer 歸還、優雅停機、C100K）

## License

MIT