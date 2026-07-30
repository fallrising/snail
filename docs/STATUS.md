# rudis 開發狀態

> 最後更新：2026-07-30

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | |
| **M1 完整語義 + 多核** | ✅ 完成 | **C10K gate PASS**（1w / 多 w） |
| **M2 承壓層** | 🟡 進行中 | **C100K hold PASS**；全活躍吞吐提升中 |
| **M3 極限** | ⬜ 未開始 | io_uring、C1M |

## C10K

| 閘道 | 結果 |
|---|---|
| Gate hold10K+active64（1 worker） | **PASS** p99 ~1.3–1.5ms |
| 同閘道多 worker（2w / 4w） | **PASS** p99 ~1.5–2.4ms（偶發噪音） |
| Stress 10K 全活躍（pipeline=1） | 資訊；~100K req/s，p99 百 ms（Little's law） |
| Stress 10K + pipeline=16 | 資訊；**~800K req/s** |

## 本輪（吞吐熱路徑）

- 讀路徑：直接讀入 `BytesMut` spare capacity（去掉 16KiB 中間緩衝）
- 跨 shard 等待：adaptive spin（≤64 waiters 維持 48 次深 spin；全活躍時縮短避免 thrash）
- `rudis-bench --pipeline/-P`：pipelined 全活躍壓測；`PIPELINE=` 傳入 `bench-c10k.sh`
- 達 p99&lt;5ms@10K in-flight 約需 ~2M req/s（設計目標）；當前 pipelined ~0.8M

## 下一步

1. 繼續抬全活躍吞吐（writev 批次、更緊 RPC、io_uring）
2. C1M hold
3. 無鎖 cross-shard completion
