# rudis 技術設計文檔

| 項目 | 內容 |
|---|---|
| 版本 | v0.1（設計稿） |
| 範圍 | 單機、單進程、純記憶體、RESP2 over TCP |
| 代號 | `rudis`（Rust In-Memory Redis-Compatible Server） |

## 概述

用 Rust 實作常駐記憶體 KV 服務，對外講 **Redis 協議（RESP2 over TCP）**，`redis-cli`、`redis-benchmark`、主流 Redis SDK 可直接連。

### 目標

- 支持 Redis 五大核心資料結構：String / List / Hash / Set / Sorted Set
- Generic key 命令（TTL、DEL、SCAN…）與 server 命令（PING、INFO…）
- **C10K 硬性達成**；**C1M 設計上限**（thread-per-core share-nothing）

### 非目標（MVP 不做）

| 機制 | 原因 |
|---|---|
| 持久化（RDB / AOF） | 純記憶體，重啟即空 |
| 主從複製 / Cluster / Sentinel | 單機單進程 |
| Pub/Sub、Keyspace notification | 連線模型複雜化 |
| MULTI/EXEC、Lua/Function | 同上 |
| 阻塞命令（BLPOP 等） | 需 per-key waiter 隊列 |
| AUTH / ACL / TLS | 假設內網信任邊界 |
| LRU/LFU eviction | MVP 只做 noeviction |
| Stream / HyperLogLog / Geo / Bitmap | 見延伸路線 |

## 架構決策

### 併發模型：thread-per-core share-nothing（方案 B）

- 每個 Shard 被恰好一個 worker 獨占，**零鎖**
- 本地命令走 fast path；跨 shard 走 MPSC + oneshot
- 每 worker 一個 tokio current-thread runtime

### 傳輸：RESP2 over 裸 TCP（非 HTTP）

相容 redis-cli 生態；天然支持 pipeline。

## 設計不變量（Invariants）

| ID | 內容 |
|---|---|
| I1 | Shard 內部狀態只能被 owner worker 訪問 |
| I2 | Shard 上命令處理純同步，不得 await |
| I3 | RefCell borrow 不跨 await |
| I4 | 連線內回覆嚴格 FIFO 保序 |
| I5 | 跨 shard 多 key 命令無全局原子性 |

## 模組結構

```
rudis/
├── src/
│   ├── main.rs / lib.rs / config.rs / error.rs / telemetry.rs
│   ├── runtime/     # bootstrap, worker, router, shutdown
│   ├── net/         # listener, connection, buffer
│   ├── protocol/    # frame, parser, encoder
│   ├── command/     # parse, dispatcher, apply, string/list/hash/set/zset/keys/server
│   └── storage/     # shard, value, expire, stats
├── tests/
├── scripts/
└── docs/
```

## 里程碑

| 里程碑 | 內容 | 驗收 |
|---|---|---|
| M0 | 骨架：parser、GET/SET/DEL/PING | redis-cli 可用 |
| M1 | 附錄 A 命令、TTL、多核路由、保序 | 命令黑盒全綠、C10K |
| M2 | buffer pool、backpressure、優雅停機 | C100K |
| M3 | io_uring、sysctl、C1M 持有器 | C1M |

## 延伸路線（附錄 B）

1. 緊湊編碼（listpack / SSO）
2. ZSet skiplist（帶 span）
3. 阻塞命令（BLPOP 族）
4. Pub/Sub
5. MULTI/EXEC（同 shard 先行）
6. 淘汰策略（近似 LRU）
7. RESP3
8. HTTP 網關（獨立進程）
9. 持久化（AOF 先於 RDB）
10. 熱 shard 治理（slot 遷移）

---

> 當前實作進度與待辦事項見 [STATUS.md](./STATUS.md)。