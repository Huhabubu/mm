# 进度记录（2026-11-25 暂停）

分支 `exp/usdc-perp-glft-min`，基于 `main`；暂停前领先 `main` 1 个提交（`f4ad6c0`）。按使用者指示暂停，以下是交接状态。

## 已完成（已在分支上）

- `README.md`：实验设计、数据、GLFT 公式、输出口径、已知限制。
- `glft_core.py`：numba GLFT 系数、强度拟合、环形缓冲、post-only 报价。
- `test_glft_core.py`：单元测试。**目前还没有 workflow，这些测试未在 GitHub Actions 上跑过。**

## 已核对、准备复用（来源未改动）

- Kaggle 运行链路：`exp/tokyo-l2-glft` 上的 `.github/workflows/tokyo-l2-kaggle.yml` 与 `kaggle/launcher.py`，包括 token 认证、notebook 私有性校验、push/轮询/取回输出、secret 扫描。这两个文件不在 `main` 上，本分支也没有。
- Tardis 下载：`scripts/check_okx_hft_data.py` 的 `SCHEMAS` 与 `extract_window(kind, day, end_us, target, max_rows, exchange=, symbol=)`。URL 格式为 `{BASE_URL}/{exchange}/{kind}/YYYY/MM/DD/{symbol}.csv.gz`；流式解压 gzip，按 `local_timestamp` 截断，并检查快照和单调性。
- 分段转换：`scripts/tardis_segmented_hbt.py::convert_fuse_parts`，调用 `hftbacktest.data.utils.tardis.convert_fuse(trades, L2, book_ticker, ...)`，之后执行 `validate_event_order`。
- `main` 上的 `full-day-hbt-pilot.yml` 在 GitHub runner 上安装 `hftbacktest==2.4.4`，直接下载 Tardis 免费日数据并回测全天。

## 待处理问题

- README 数据表只列了 ETHUSDC 的 `trades` + `incremental_book_L2`，但 `convert_fuse` 还需要 ETHUSDC 的 `book_ticker`，README 和下载清单都要补上。
- `extract_window` 默认 `max_rows=500_000`，ETHUSDC 全天 L2 很可能超出。要先查 Tardis 文件的实际大小再放宽。
- 现有 Kaggle workflow 要求名为 NOTEBOOK_NAME 的 notebook 已经存在且只有一个，而 `usdc-perp-glft-min` 还没创建。需要加一条“不存在就以私有方式新建”的路径，并保留私有性校验。
- `launcher.py` 强制要求 `--collector` 参数并打包 collector 代码，本实验用不到，需要精简版，或者把该参数改成可选。
- 暂停时还没读完：`inspect_tardis.BASE_URL` 的具体值，以及 `g0_glft_event_driven.py` 中 hftbacktest 2.4.4 的资产配置和成交读取写法（那次读取的结果没有返回）。
- 还要确认 `mm` 仓库的 secret `KAGGLE_API_TOKEN` 可用（tokyo workflow 用的是同名 secret）。

## 下一步（小批量提交，每次推送后核对文件树）

1. 读完 hftbacktest API 用法和 `BASE_URL`；查 2026-11-01 ETHUSDC、ETHUSDT 的文件大小。
2. 提交 `kaggle_task.py`（下载 → 转换 → 4 组参数回测 → `report.json`），并先让 GitHub runner 跑通单元测试。
3. 提交精简 launcher 和 `.github/workflows/usdc-perp-glft-min.yml`（推送即触发 Kaggle）。
4. 取回 `report.json`、`run.log`、`job_status.json`，确认 `git_sha` 一致后再汇报结果。