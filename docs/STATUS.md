# rudis 開發狀態

> 最後更新：2026-07-29

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | |
| **M1 完整語義 + 多核** | 🟡 ~90% | 命令齊全；10K 全活躍延遲未達標（吞吐瓶頸） |
| **M2 承壓層** | 🟡 進行中 | **C100K hold PASS**；mio / jemalloc / 優雅停機 |
| **M3 極限** | ⬜ 未開始 | io_uring、C1M |

## 近期完成

### perf：熱路徑 + reactor O(n) 修復
- `get_string` 單次查找；shard 統計 batch（`note_*` / `flush_stats`）
- PONG/OK/NullBulk/Bulk 直接編碼；Linux `TCP_QUICKACK`
- 預設 `SET` 快路（略過 nx/xx/get 多次 lookup）
- **reactor 不再每 turn 掃描全部連線的 async oneshot**（改 waiter 清單）→ 單 worker 10K p99 ~315ms→~187ms

### C100K hold（loopback spread）✅

```
CLIENTS=100000 LOOPBACK_SPREAD=64 ./scripts/bench-c100k.sh all
→ connected=100000/100000, ping_err=0  PASS
```

## 壓測摘要（本輪）

| 場景 | 結果 |
|---|---|
| 1 worker / 64 連線 | **PASS** p99 ~1.7ms |
| 多 worker / 64 連線 | 偶發 FAIL ~9ms（跨 shard oneshot） |
| 10K hold | PASS |
| 100K hold | PASS |
| 10K 全活躍（無 pipeline） | FAIL ~170ms p99 / ~120K req/s |

### 為什麼 10K 全活躍 p99&lt;5ms 極難

無 pipeline、每連線同時 in-flight 1 個請求時：

`所需吞吐 ≈ 10000 / 0.005 = 2M req/s`

目前單機 ~120K req/s ⇒ Little’s law 預測平均延遲 ~80ms，與實測同量級。  
下一步要嘛再抬 10×+ 單機吞吐（io_uring / 更深熱路徑），要嘛調整 SLO（例如 paced 負載、有限 concurrency、或允許 pipeline）。

## 下一步

1. 繼續抬單核/多核有效吞吐（跨 shard 路徑、解析、writev）
2. 評估 C10K SLO：hold 10K + 中等 concurrency 延遲 vs 全活躍
3. M3：io_uring、C1M hold

```bash
cargo test --release
CLIENTS=10000 ./scripts/bench-c10k.sh all
CLIENTS=100000 LOOPBACK_SPREAD=64 ./scripts/bench-c100k.sh all
```
