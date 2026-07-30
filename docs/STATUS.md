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
| Gate hold10K+active64（1 worker） | **PASS** p99 ~1.0–1.5ms |
| 同閘道多 worker（2w / 4w） | **PASS** p99 ~2–4ms（偶發噪音） |
| Stress 10K 全活躍（pipeline=1） | 資訊；~70K req/s，p99 百 ms（Little's law） |
| Stress 10K + pipeline=16 | 資訊；**~530–550K req/s** |
| Peak（256 conn + P16） | 資訊；**~1.1–1.3M req/s** |

## 本輪（writev + 熱路徑）

- **writev OutBuf**：小回覆合併進 contiguous tail；大 bulk（>64B）零拷貝分段 `write_vectored`
- **GET/SET 快路徑解析**：跳過 Frame/Command 建構，直接 apply + encode
- **熱路徑去 RefCell 重入**：reactor 一次 borrow shards，傳入 `drive` / `dispatch_on`
- 去掉每讀 `TCP_QUICKACK` setsockopt（降 syscall）
- epoll events capacity 1024 → 4096
- Peak pipelined：**~0.76M → ~1.1–1.3M** req/s；10K×P16 仍受 fd/調度限制（~0.55M）
- 達 p99&lt;5ms@10K in-flight 約需 ~2M req/s；下一步 io_uring / 更緊 RPC

## 下一步

1. 繼續抬全活躍吞吐（io_uring、多 worker 擴展時的跨 shard RPC）
2. C1M hold
3. 無鎖 cross-shard completion；多 w gate 偶發噪音
