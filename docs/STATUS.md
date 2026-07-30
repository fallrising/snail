# rudis 開發狀態

> 最後更新：2026-07-30

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | |
| **M1 完整語義 + 多核** | ✅ 完成 | **C10K gate PASS**（1w / 多 w） |
| **M2 承壓層** | 🟡 進行中 | **C100K hold PASS**；全活躍吞吐仍軟 |
| **M3 極限** | ⬜ 未開始 | io_uring、C1M；全活躍吞吐 |

## C10K

| 閘道 | 結果 |
|---|---|
| Gate hold10K+active64（1 worker） | **PASS** p99 ~1.3ms |
| 同閘道多 worker（2w / 4w） | **PASS** p99 ~1.5ms |
| Stress 10K 全活躍 | 資訊；p99 百 ms 級 |

## 本輪（reply wake + cooperative spin）

- 跨 shard 回覆後 **wake origin** worker（與 inbox wake 共用 coalesce）
- 等待 oneshot 時 **cooperative spin**：邊 drain 本地 shard RPC、邊 harvest，避免多 worker 互相卡住
- 短暫 mio park（20µs）+ 偶發 `yield_now` 兼顧 LocalSet multi-gather
- 多 worker p99：~11ms → **~1.5ms**（gate PASS）

## 下一步

1. 全活躍吞吐（pipeline 壓測、writev、io_uring）
2. C1M hold
3. 進一步壓低跨 shard RPC 開銷（無鎖 completion / 更緊的 channel）
