# rudis 開發狀態

> 最後更新：2026-07-29

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | 單/多 worker、RESP2、基本 TCP |
| **M1 完整語義 + 多核** | 🟡 ~90% | 五大結構 + COMMAND_TABLE；C10K 全負載 p99 未達標 |
| **M2 承壓層** | 🟡 進行中 | mio 反應器、優雅停機、C10K hold、C100K 工具就緒；ulimit 限制下 50K+ 需 root |
| **M3 極限** | ⬜ 未開始 | io_uring、C1M 持有器 |

## 近期完成（2026-07-29 續）

### 熱路徑吞吐 ✅

- GET/SET/PING parse 快路（避免 command-name `String` 分配）
- `COMMAND_MAP`（ahash）取代線性表掃描
- `primary_key` 借出路由鍵，去掉 `Vec` 分配
- 每連線重用 `Dispatcher`（不再每命令 clone Arc）

### C100K 工具 + sysctl ✅

- `rudis-bench --hold`：開 N 連線、週期 PING、連線成功率判定
- `scripts/bench-c100k.sh`
- `scripts/sysctl-tuning.sh`：apply 前備份、`restore` 還原

### 壓測結果

| 場景 | 結果 |
|---|---|
| 64 連線延遲 | **PASS** p99 ≈ **4.3 ms**、零錯誤 |
| 10K 連線 hold（10s PING） | **PASS** 10000/10000、ping_err=0 |
| 50K hold（ulimit 65536） | FAIL ~28K 連上（FD 不夠；需 `ulimit -n`≥2×clients 或 root sysctl） |
| 10K 全活躍延遲 | FAIL p99 ~150 ms（吞吐 ~120–150K req/s；Little's law 約需 ≥2M req/s） |

## 測試

```bash
cargo test   # 28 tests
```

## 下一步

1. 繼續拉高吞吐（緊湊編碼 / 更少分配 / io_uring）以逼近 10K 全活躍 p99
2. 在足夠 FD 環境跑滿 C100K hold（`ulimit -n 1048576` + sysctl）
3. C1M 持有器與 sysctl 實機定稿

詳見 [README.md](../README.md)、[design.md](./design.md)。
