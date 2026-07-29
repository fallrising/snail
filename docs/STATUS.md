# rudis 開發狀態

> 最後更新：2026-07-29

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | |
| **M1 完整語義 + 多核** | 🟡 ~95% | **C10K gate PASS**（1w hold10K+active64）；多 worker 跨 shard 尾延遲仍高 |
| **M2 承壓層** | 🟡 進行中 | **C100K hold PASS** |
| **M3 極限** | ⬜ 未開始 | io_uring、C1M；全活躍 p99 列吞吐目標 |

## C10K 口徑

| 閘道 | 條件 | 結果 |
|---|---|---|
| **Gate** | hold 10K + ACTIVE=64，p99&lt;5ms（預設 1 worker） | **PASS** p99 ~1.4ms |
| **Stress** | 10K 全活躍 `--soft` | ~180ms p99 / ~68K req/s（資訊） |

```bash
./scripts/bench-c10k.sh all
# rudis-bench -c 10000 --active 64 -n 200 --p99-ms 5
```

多 worker 下同閘道 p99 常 ~10ms（跨 shard oneshot）；生產仍可用多核，延遲閘以單核為準。

## 近期完成

- C10K SLO：`--active`（大量 idle + 中等 concurrency）
- 跨 shard waker：`OnceLock`（去掉每請求 Mutex）
- `rudis-bench --soft` / `--p99-ms`

## 下一步

1. 多 worker 跨 shard 路徑（batch / 更少 wake）壓低尾延遲
2. 抬吞吐 → stress p99
3. M3：io_uring / C1M
