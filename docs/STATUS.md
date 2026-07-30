# rudis 開發狀態

> 最後更新：2026-07-30

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | |
| **M1 完整語義 + 多核** | ✅ 完成 | **C10K gate PASS**（1w / 多 w） |
| **M2 承壓層** | 🟡 進行中 | **C100K hold PASS**；全活躍吞吐提升中 |
| **M3 極限** | 🟡 起步 | io_uring 批次寫（opt-in）；C1M hold 腳本 |

## C10K

| 閘道 | 結果 |
|---|---|
| Gate hold10K+active64（1 worker） | **PASS** p99 ~1.0–1.5ms |
| 同閘道多 worker（2w / 4w） | **PASS** 為主；偶發 p99 噪音 >5ms |
| Stress 10K 全活躍（pipeline=1） | 資訊；~70K req/s |
| Stress 10K + pipeline=16 | 資訊；**~0.5–0.55M req/s** |
| Peak（256 conn + P16） | 資訊；**~1.1–1.2M req/s** |

## 本輪（io_uring 起步 + C1M 腳本）

- **`io-uring` 依賴 + `UringBatch`**：就緒連線的 writev 可合併進一次 `io_uring_enter`
- **預設關閉**（避免小 fan-out 回歸）：`RUDIS_IO_URING=1` 開啟；讀路徑仍為 sync drain（ET 安全）
- 下一步吞吐：always-in-flight Recv/Send（資料 fd 不再掛 epoll）才可能接近 ~2M@10K
- **`scripts/bench-c1m.sh`**：C1M hold 驗收腳本（需高 ulimit + sysctl；本機尚未跑滿 1M）

## 下一步

1. Completion 式 io_uring（always-in-flight recv/send）抬 10K 全活躍吞吐
2. 實跑並壓穩 C1M hold
3. 無鎖 cross-shard completion；多 w gate 偶發噪音
