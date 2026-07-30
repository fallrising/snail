# rudis 開發狀態

> 最後更新：2026-07-30

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | |
| **M1 完整語義 + 多核** | ✅ 完成 | **C10K gate PASS**（1w / 多 w） |
| **M2 承壓層** | 🟡 進行中 | **C100K hold PASS**；全活躍吞吐提升中 |
| **M3 極限** | 🟡 進行中 | completion io_uring（AcceptMulti+eventfd）；C1M 腳本 |

## C10K（預設 mio）

| 閘道 | 結果 |
|---|---|
| Gate hold10K+active64（1 worker） | **PASS** p99 ~1.0–2.5ms |
| 同閘道多 worker（2w / 4w） | **PASS** 為主；偶發 p99 噪音 >5ms |
| Stress 10K + pipeline=16 | 資訊；**~0.4–0.55M req/s** |
| Peak（256 conn + P16） | 資訊；**~1.0–1.1M req/s** |

## `RUDIS_IO_URING=1`（completion reactor）

- 資料 fd：**always-in-flight Recv/Send**（不掛 epoll）
- **AcceptMulti + eventfd wake** 同環；已移除 mio 與 50µs CQ timeout
- 1w gate：**PASS**（本機 ~1.9ms）
- Peak 256×P16：本機 **~1.2M**（優於同機 mio ~1.05M）
- 10K×P16：ring 32K 後與 mio 持平（~0.43M）；尚未達 ~2M
- 多 w：可用；gate 偶發噪音與 mio 類似

## 下一步

1. 抬全活躍吞吐（shared hot path / provided buffers / SQPOLL）向 ~2M@10K
2. 實跑並壓穩 C1M hold
3. 無鎖 cross-shard completion；多 w gate 偶發噪音
