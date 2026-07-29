# rudis 開發狀態

> 最後更新：2026-07-29

## 里程碑進度

| 里程碑 | 狀態 | 說明 |
|---|---|---|
| **M0 骨架** | ✅ 完成 | 單/多 worker、RESP2 解析/編碼、GET/SET/DEL/PING、基本 TCP 服務 |
| **M1 完整語義 + 多核** | 🟡 進行中（~90%） | 五大結構命令齊全；`COMMAND_TABLE` 已補全；跨 shard 路由完成 |
| **M2 承壓層** | 🟡 進行中 | mio 反應器、buffer 歸還、maxclients、優雅停機已落地；C10K p99 / C100K 未達標 |
| **M3 極限** | ⬜ 未開始 | io_uring、C1M 持有器、sysctl 定稿 |

## 近期完成（2026-07-29）

### P0 — mio/epoll 連線反應器 ✅

- `src/net/reactor.rs` 改為 **mio Poll**：只處理 ready FD（O(ready)，非 O(conns)）
- Shard apply + active expire 折入反應器迴圈；跨 worker 以 `mio::Waker` 喚醒
- Immediate 回覆快路：無 in-flight async 時直接 encode，跳過 pending 隊列
- 空閒 read buffer 歸還 pool；`maxclients` 回錯誤後關閉

### P0 — 優雅停機 ✅

- `SIGINT` / `SIGTERM` → broadcast → 停 accept → 連線 drain → deadline → worker 退出
- 測試：`graceful_shutdown_on_sigterm`

### P1 — M1 收尾 ✅

- `COMMAND_TABLE` 與 parse_body 對齊
- `commands_list` / `commands_set` / `commands_server` 黑盒測試

### C10K 重測（mio 反應器後）

| 客戶數 | 吞吐 | p50 | p99 | 判定 |
|---|---|---|---|---|
| 64 | ~85K req/s | 0.31 ms | 6.0 ms | 接近 |
| 1,000 | ~126K req/s | 4.2 ms | 25 ms | FAIL |
| 10,000 | ~121–143K req/s | 53–65 ms | **147–156 ms** | FAIL |

Little's law：10K 並行 in-flight × 無 pipeline ⇒ p99&lt;5ms 約需 **≥2M req/s**。  
當前 ~120–150K req/s，平均延遲 ≈ N/throughput ≈ 70ms，與實測吻合。後續需把單機吞吐再拉一個數量級，或調整驗收口徑（持有 10K + 限速測延遲）。

## 測試

```bash
cargo test   # 28 tests
```

## 下一步

1. **吞吐**：紧湊编码、减少热路径分配、可选 io_uring（M3）
2. **C100K** 连线持有验收
3. sysctl 调优定稿、C1M 持有器

## 目录

见 [design.md](./design.md)。完整架构与语义差异见 [README.md](../README.md)。
