# rudis 開發狀態

> 最後更新：2026-07-29

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | |
| **M1 完整語義 + 多核** | 🟡 ~90% | 命令齊全；10K 全活躍延遲未達標 |
| **M2 承壓層** | 🟡 進行中 | **C100K hold PASS**；優雅停機 / mio 反應器 / jemalloc 已落地 |
| **M3 極限** | ⬜ 未開始 | io_uring、C1M |

## 近期完成

### perf：jemalloc + 本地 GET/SET/PING 快路 ✅
### C100K hold（loopback spread 繞過 ephemeral port 上限）✅

```
CLIENTS=100000 LOOPBACK_SPREAD=64 ./scripts/bench-c100k.sh all
→ connected=100000/100000, ping_err=0  PASS
```

單一目的地 `127.0.0.1:port` 約受 ~28k ephemeral port 限制；`--loopback-spread N` 分散到 `127.0.0.1..N`（server bind `0.0.0.0`）。

### 壓測摘要

| 場景 | 結果 |
|---|---|
| 64 連線延遲 | 曾 PASS ~4.3ms；負載下偶發 ~9ms |
| 10K 連線 hold | PASS |
| **100K 連線 hold** | **PASS**（spread=64） |
| 10K 全活躍延遲 | FAIL ~150ms（吞吐瓶頸） |

## 下一步

1. 吞吐 → 10K 全活躍 p99&lt;5ms  
2. C1M hold + io_uring  

```bash
cargo test
CLIENTS=100000 LOOPBACK_SPREAD=64 ./scripts/bench-c100k.sh all
```
