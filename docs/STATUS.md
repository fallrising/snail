# rudis 開發狀態

> 最後更新：2026-07-13

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | 單/多 worker、RESP2 解析/編碼、GET/SET/DEL/PING、基本 TCP 服務 |
| **M1 完整語義 + 多核** | 🟡 進行中（~60%） | 五大結構基本命令已實作；跨 shard 異步路由待完善 |
| **M2 承壓層** | ⬜ 未開始 | buffer pool、backpressure、優雅停機、C100K 驗收 |
| **M3 極限** | ⬜ 未開始 | io_uring、C1M 持有器、sysctl 定稿 |

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
- Set：SADD/SINTER/SUNION/SDIFF…
- ZSet：ZADD/ZRANGE/ZRANK/ZPOPMIN…（ZRANGEBYSCORE 為 placeholder）
- Keys：DEL/EXPIRE/TTL/SCAN/RENAME…
- Server：PING/ECHO/HELLO/INFO/DBSIZE/FLUSHDB/CONFIG GET

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

## 測試覆蓋

| 測試 | 狀態 |
|---|---|
| `protocol/parser` 單元測試 | ✅ |
| `tests/protocol_test.rs` | ✅ |
| `tests/integration_test.rs`（TCP 黑盒 PING/GET/SET） | ✅ |
| `tests/commands/*` 逐命令黑盒 | ⬜ 待補 |
| C10K 壓測（p99 < 5ms） | ⬜ 待跑 |

```bash
cargo test   # 目前 5 項全綠
```

## 已知限制與語義差異

詳見 [README.md](../README.md#semantic-differences-from-standalone-redis)。核心項：

- **I5**：跨 shard 多 key 命令無全局原子性
- **SCAN**：HashMap 桶位游標，rehash 時可能漏鍵
- **記憶體統計**：啟發式估算，±20% 誤差
- **跨 shard dispatcher**：`MultiDecompose` / `MultiGather` 同步路徑不完整，多 worker 下 MGET/DEL/SINTER 可能靜默錯誤

## 下一步（優先級排序）

### P0 — 架構正確性

1. **跨 shard 異步 dispatcher**
   - `MGET` / `MSET` / `DEL` / `EXISTS` 在 conn task 側 scatter-gather + await
   - `SINTER` / `SUNION` / `SDIFF` 異步取全量後本地計算
   - `RENAME` 跨 shard 回 CROSSSLOT 風格錯誤

### P1 — M1 命令補齊

2. 實作 `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` 真實語義
3. `HSCAN` / `SSCAN` 游標步進
4. `COMMAND` 由 CommandSpec 表自動生成
5. 新增 `tests/commands/` 黑盒測試（每型別一檔）

### P2 — M1 併發驗收

6. C10K 口徑壓測：`redis-benchmark` 或 memtier，10K 全活躍、GET/SET 8:2、無 pipeline、p99 < 5ms
7. per-shard ops 指標暴露（驗證負載均衡）

### P3 — M2 承壓層

8. 空閒連線 read buffer 歸還 pool（C1M 記憶體預算關鍵）
9. 優雅停機：broadcast → drain → deadline
10. `maxclients` 超限回 `-ERR max number of clients reached` 後關閉
11. C100K 連線持有驗收

### P4 — M3 極限

12. `scripts/sysctl-tuning.sh` 定稿（含還原）
13. 自寫 C1M 連線持有器（PING 心跳 + RTT 統計）
14. 可選：`io_uring` feature 切換

## 目錄對照

```
src/
├── protocol/     # RESP2 解析與編碼
├── storage/      # Shard、Value、ExpireIndex
├── command/      # 命令解析、路由、語義實作
├── runtime/      # Worker、bootstrap、路由 mesh
├── net/          # Listener、Connection、BufferPool
├── config.rs     # 配置加載與校驗
├── error.rs      # 三層錯誤映射
└── telemetry.rs  # tracing、INFO 聚合
```

完整架構設計見技術設計文檔（專案評審稿 v0.1）。