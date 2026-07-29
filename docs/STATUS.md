# rudis 開發狀態

> 最後更新：2026-07-29

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | |
| **M1 完整語義 + 多核** | 🟡 ~95% | **C10K gate PASS**（1w）；多 worker 跨 shard p99 仍 ~11ms |
| **M2 承壓層** | 🟡 進行中 | **C100K hold PASS** |
| **M3 極限** | ⬜ 未開始 | io_uring、C1M；全活躍吞吐 |

## C10K

| 閘道 | 結果 |
|---|---|
| Gate hold10K+active64（1 worker） | **PASS** p99 ~1.5–2.4ms |
| 同閘道多 worker | FAIL p99 ~11ms（跨 shard oneshot） |
| Stress 10K 全活躍 | 資訊；p99 百 ms 級 |

## 本輪（`354129c`）

- 跨 shard：wake **coalesce**、TOKEN_WAKE 立即 drain、inbox drain 零 Vec 分配
- 曾試 reactor 內 spin 等 oneshot → 多 worker 更差，已撤回

## 下一步

1. 多 worker 跨 shard（更低延遲 RPC / 連線親和）
2. 全活躍吞吐（pipeline 壓測、writev、io_uring）
3. C1M hold
