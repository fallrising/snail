# rudis 開發狀態

> 最後更新：2026-07-16

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | 單/多 worker、RESP2 解析/編碼、GET/SET/DEL/PING、基本 TCP 服務 |
| **M1 完整語義 + 多核** | 🟡 進行中（~80%） | 五大結構命令基本齊全；跨 shard 異步路由已完成；C10K p99 未達標 |
| **M2 承壓層** | 🟡 起步 | 壓測工具就緒；連線反應器、buffer 歸還、優雅停機待做 |
| **M3 極限** | ⬜ 未開始 | io_uring、C1M 持有器、sysctl 定稿 |

## 近期完成（2026-07-13 ~ 2026-07-16）

### P0 — 架構正確性 ✅

- 跨 shard 異步 dispatcher（`spawn_local` + scatter-gather）
  - `MGET` / `MSET` / `MSETNX` / `DEL` / `EXISTS`
  - `SINTER` / `SUNION` / `SDIFF`（含 `*STORE` 變體）
- `RENAME` 跨 shard 回 CROSSSLOT 風格錯誤
- `command_keys()` 路由鍵提取補全

### P1 — 命令補齊 ✅

- `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` 真實語義（含 `-inf`/`+inf`、exclusive bound、`LIMIT`）
- `HSCAN` / `SSCAN` 游標步進（sorted key、`COUNT` 語義）
- `COMMAND` / `COMMAND INFO` / `COMMAND COUNT` 由 `CommandSpec` 表自動生成
- 黑盒測試：`tests/commands_*.rs`（string / hash / zset / keys）+ `tests/multi_shard_test.rs`

### P2 — 併發驗收 🟡

- 新增 `rudis-bench`（tokio 異步 C10K 客戶端）與 `scripts/bench-c10k.sh`
- C10K 壓測已跑（12 workers / 12 shards）：

| 指標 | 結果 | 目標 |
|---|---|---|
| 連線數 | 10,000 | 10,000 |
| 總請求 | 1,000,000 | — |
| 錯誤 | **0** | 0 |
| 吞吐 | ~156K req/s | — |
| p50 | 16.4 ms | — |
| p99 | **147.6 ms** | **< 5 ms** |
| 判定 | **FAIL** | PASS |

- 單連線基線：p99 ≈ 0.29 ms（熱路徑正常；瓶頸在 10K 連線調度）
- per-shard ops 指標暴露：✅ `INFO STATS` 提供各 shard commands / keys / expires / memory

### P3 — 可觀測性與協議正確性 ✅

- `INFO` 改用 process-wide atomic snapshot，任一 worker 均可讀取全局統計
  - aggregate：commands / hits / misses / expired / flushed / keys / expires / memory
  - per-shard：`rudis_shard_<id>_commands` / keys / expires / used_memory
- 修正 RESP bulk length 在 TCP 封包切於 `$` 或 CRLF 中間時的 parser state corruption
- 修正相同 deadline 的多個 key 共用 expiry sequence、導致 expire index 互相覆蓋
- active expire 現在同步更新 memory 與 telemetry gauges
- 測試啟動改用 Cargo binary discovery，乾淨 checkout 可直接 `cargo test`

## 已實作模組

### 協議層 (`src/protocol/`)

- RESP2 增量解析器（Array / inline 命令）
- 協議加固：`max_bulk_len`、`max_multibulk`、`max_inline_len` 上限
- Reply 編碼器（靜態常量快路）

### 儲存層 (`src/storage/`)

- Shard：主字典 + 過期索引 + 記憶體記帳
- Value：String / List / Hash / Set / ZSet（雙結構有序集合）
- Lazy + Active 過期（100ms ticker，每輪 ≤1000 key）

### 命令層 (`src/command/`)

- 路由類：Local / Key / MultiDecompose / MultiGather / Broadcast / CursorTargeted
- String：GET/SET/INCR/MGET/MSET/APPEND/GETRANGE…
- List：LPUSH/RPOP/LRANGE/LTRIM…
- Hash：HSET/HGET/HGETALL/HINCRBY/HSCAN…
- Set：SADD/SINTER/SUNION/SDIFF/SSCAN…
- ZSet：ZADD/ZRANGE/ZRANGEBYSCORE/ZRANK/ZPOPMIN…
- Keys：DEL/EXPIRE/TTL/SCAN/RENAME（跨 shard CROSSSLOT）…
- Server：PING/ECHO/HELLO/INFO/DBSIZE/FLUSHDB/CONFIG GET/COMMAND

### 運行時 (`src/runtime/`)

- thread-per-core worker（tokio current-thread + LocalSet）
- SO_REUSEPORT 多 listener 負載均衡
- ShardMap 路由（隨機 hash 種子防 hash-flooding）
- 跨 worker MPSC channel + oneshot 回覆

### 網路層 (`src/net/`)

- 連線狀態機：讀→解析→派發→保序→寫
- Pipeline FIFO 保序（I4）
- 兩級 backpressure（`out_buf_soft` / `out_buf_hard`）
- 分級 buffer pool（4K / 16K）
- write-first + `readable().await` 空閒休眠

## 測試覆蓋

| 測試 | 狀態 |
|---|---|
| `protocol/parser` 單元測試 | ✅ |
| `tests/protocol_test.rs` | ✅ |
| `tests/integration_test.rs` | ✅ |
| `tests/multi_shard_test.rs`（MGET/DEL/SINTER/RENAME 跨 shard） | ✅ |
| `tests/commands_string.rs` | ✅ |
| `tests/commands_hash.rs` | ✅ |
| `tests/commands_zset.rs` | ✅ |
| `tests/commands_keys.rs` | ✅ |
| RESP 逐 byte 增量解析 | ✅ |
| 相同 deadline 多 key 過期 | ✅ |
| 多 worker `INFO` shard metrics | ✅ |
| C10K 壓測（p99 < 5ms） | 🟡 已跑，p99 未達標 |

```bash
cargo test   # 目前 21 項全綠
```

## 已知限制與語義差異

詳見 [README.md](../README.md#semantic-differences-from-standalone-redis)。核心項：

- **I5**：跨 shard 多 key 命令無全局原子性
- **SCAN**：HashMap 桶位游標，rehash 時可能漏鍵
- **記憶體統計**：啟發式估算，±20% 誤差
- **C10K 尾延遲**：每連線一個 `spawn_local` task，10K 連線下協程調度開銷大（p99 ~148ms）

## 下一步（優先級排序）

### P0 — C10K p99 達標（M2 前置）

1. **每 worker 連線反應器**：取代 per-connection `spawn_local`，單 epoll 迴圈驅動多連線

### P1 — M1 收尾

2. 補齊 `COMMAND_TABLE` 剩餘命令註冊
3. 更多 `tests/commands/` 覆蓋（list / set / server）

### P2 — M2 承壓層

4. 空閒連線 read buffer 歸還 pool（C1M 記憶體預算關鍵）
5. 優雅停機：broadcast → drain → deadline
6. `maxclients` 超限回 `-ERR max number of clients reached` 後關閉
7. C100K 連線持有驗收

### P3 — M3 極限

8. `scripts/sysctl-tuning.sh` 定稿（含還原）
9. 自寫 C1M 連線持有器（PING 心跳 + RTT 統計）
10. 可選：`io_uring` feature 切換

## 目錄對照

```
snail/
├── src/
│   ├── protocol/     # RESP2 解析與編碼
│   ├── storage/      # Shard、Value、ExpireIndex
│   ├── command/      # 命令解析、路由、語義實作
│   ├── runtime/      # Worker、bootstrap、路由 mesh
│   ├── net/          # Listener、Connection、BufferPool
│   ├── bin/          # rudis-bench 壓測客戶端
│   ├── config.rs
│   ├── error.rs
│   └── telemetry.rs
├── tests/
│   ├── common/       # 測試共用 helper
│   ├── commands_*.rs # 逐型別黑盒測試
│   └── multi_shard_test.rs
├── scripts/
│   ├── bench.sh
│   ├── bench-c10k.sh
│   └── sysctl-tuning.sh
└── docs/
    ├── design.md
    └── STATUS.md
```

完整架構設計見 [design.md](./design.md)（專案評審稿 v0.1）。
