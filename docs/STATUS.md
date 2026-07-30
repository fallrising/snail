# rudis 開發狀態

> 最後更新：2026-07-30

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | |
| **M1 完整語義 + 多核** | ✅ 完成 | **C10K gate PASS**（1w / 多 w） |
| **M2 承壓層** | 🟡 進行中 | **C100K hold PASS**；單機峰值抬升 |
| **M3 極限** | 🟡 進行中 | completion io_uring；C1M 腳本待實跑 |

## C10K（預設 mio）

| 閘道 | 結果 |
|---|---|
| Gate hold10K+active64（1 worker） | **PASS** p99 ~1.6ms |
| 同閘道多 worker（2w / 4w） | **PASS** 為主；偶發 p99 噪音 >5ms |
| Peak 256×P16 | 資訊；**~1.17M req/s** |
| Peak 64×P32 | 資訊；**~2.08M req/s** |
| Stress 10K×P16 | 資訊；**~0.36–0.45M req/s** |

## `RUDIS_IO_URING=1`

- AcceptMulti + eventfd + always-in-flight Recv/Send；ring 32K
- Send 期間可並行 Recv（out_buf freeze segs）
- 1w gate：**PASS** ~2.1ms
- Peak 與 mio 持平：256×P16 ~1.17M；**64×P32 ~2.08M**
- 10K×P16：~0.45M（優於同輪 mio）；尚未達 ~2M@10K / p99&lt;5ms

## 本輪峰值優化

- 單 shard 熱路徑跳過 key hash / owner 查詢
- process_input 一次取 now；uring Recv∥Send + CQ burst drain

## 下一步（告一段落後）

1. 抬 10K 全活躍吞吐與 p99（provided buffers / 更省 per-conn 開銷）
2. 實跑並壓穩 C1M hold
3. 無鎖 cross-shard；多 w gate 噪音
