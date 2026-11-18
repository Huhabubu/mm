# Gate GLFT 核心引擎阶段二（2A/2B）代码与验收记录（2026-11-18）

## 0. 冻结边界

- **起点**：阶段一完成提交 `1b709c5e25874da91b80cc0307a5a4c16ffeb992`。
- **开发分支**：`feat/core-glft-engine-refactor`；旧 Stage 0/1/5A/5C 六日主文基准和前一阶段新统计报告不变；本次没有运行阶段三六日稳健性或跨币种新实证。
- **回归事实**：50分钟真实 Gate BTC 历史行情的逐笔订单请求多重集合、官方HBT逐笔成交、Recorder、期末含手续费权益一致。**同一纳秒内的Rust HashMap订单遍历顺序会不同**，仅在单一纳秒内对提交/撤销请求重新排序进行精确匹配；没有放宽纳秒时刻、操作类型、订单ID或价格档。不同的同纳秒操作顺序是复现局限，不能宣称字节级严格顺序一致。
- **旧引擎基准文件**：从上述冻结提交通过 `git show <SHA>:scripts/gate_origin_glft_strategy.py` 等路径独立读取，防止后续薄封装覆盖历史参考实现。

## 1. 2A 静态配置与单核重构

- `configs/contract_specs.json` 冻结 Gate BTC/SOL/DOGE/ETH 四种研究回测实际采用的 tick/合约面值/最小交易单位以及**实验假设**Maker收取2bps/Taker收取5bps，标注文件与日期证据来源。相关研究日志并未逐日核实交易所**历史真实手续费等级**，故不可称作完整历史交易所规格。Binance四个参考合约的执行规格未核实，以 `null` 明确留空，只允许作为BBO参考数据，不允许用于虚构Binance执行成本。
- `scripts/contract_specs.py` 和 `qa_gate_btc_futures.fetch_contract` 只读静态文件，未知币对/缺文件/错误单位立即报错；Tardis历史行情下载依旧属于市场数据下载，不再查Gate在线合约API。
- 运行器新 JSON 计数键 `above_threshold_checks`，同时新增 `sigma_threshold=1.645`；旧已归档数据保持冻结。
- `scripts/core_glft_engine.py` 是唯一活动HBT Gate GLFT事件驱动核心。原始GLFT、Stage3 shadow、Stage4 A/B控制、Stage5A风险侧暂停变为薄封装；`research/` 是冻结历史研究快照，本次未修改。
- **真实老模式回归**：[CI #44336851755](https://github.com/Huhabubu/mm/actions/runs/44336851755) 两个job成功：2026-11-01 BTC 10min预热+50min交易；原GLFT、双边beta1、单侧外移beta1以及Stage3影子、Stage4引用/固定加宽/波动暂停、Stage5A风险侧暂停与冻结提交的对应源码逐请求同纳秒多重集、逐笔成交、Recorder及期末含费权益完全一致。旧模式开关固定20ms、legacy half-tick、不锁盘、legacy post-only、不设深度上限。

## 2. 2B 五开关实现

- `min_requote_ns=0/20_000_000`：仅更改单边动作冷却，不改HBT的1ms请求及1ms回应延迟；诊断数组输出真正滑动1秒窗口内的最大提交/撤单请求数 `max_ops_per_sec`。
- `fit_mode=physical_1tick_poisson`：买侧/卖侧每100ms最大到达深度各用6,000环形样本，0.5/1.5/.../34.5真实tick阈值，共12,000个独立的侧时段暴露，按 `-ln(1-min(p,.999))/0.1` 还原双侧合并对称(A,k)，3个非零桶最低、10个穿透侧时段；原legacy fitter不变。新模式首次成功校准允许在10分钟之后，运行器先以无交易前可用的公共行情校准首个成功时刻，后续所有模型用同一绝对纳秒 `forced_start_ns` 入场，缺失校准的日直接报错。首个入场时刻以相对t0整数纳秒存诊断后精确恢复，避免大纪元浮点精度损失。
- Binance最新BBO本地可见时间超过250ms时把信号置零并清除激活状态；原legacy保持冻结行为。
- `wait_book_after_fill=True`：私域回报后实际持仓变化会记住最新本次成交的交易所时刻；等待**确实收到的一条Gate原生BBO**同时满足该交易所时刻及本地回报时刻的严格后继关系才能恢复提交订单；在此期间仍允许按现有逻辑撤单。单个 `feed_status==2` 不解锁。
- `post_only_clamp_mode=unified`：将库存与Binance偏移先合成为理论报价，最终仅按对侧最优价一tick做Post-Only剪裁并防止自交叉；保留 `legacy` 兼容旧引用。
- `cap_depth_at_visible_levels=True`：必须明确指明采用20档完整快照替换日，并对同侧**确实存在的**第20个非零价位设限；缺少20个可见价位或对照日使用400档增量格式均报错。没有证据证明超出20档的旧回测队列完全真实。
- `resting_time_ratio` 报告买卖两侧在本地能确认“仍存在且可撤销的HBT订单”的事件时段占比，**仅为本地保守代理指标**：尚未逐笔重建交易所实际挂单生效/取消时刻，不能当作“真实交易所在线率”。输出含 `verified_exchange_resting=false`，禁止论文误称实盘真实挂单在线时间。

## 3. 测试记录和运行入口

- [离线规则+真实旧模式门禁 #44336851755](https://github.com/Huhabubu/mm/actions/runs/44336851755)：pass。聚焦测试包括双侧成交同100ms碰撞不覆盖、1tick格点边界、泊松p=.999上限、报价截断、真实20个非零L2价位、成交/BBO先后、滑动1秒频控。
- [五开关组合真实50min烟雾测试 #44336850404](https://github.com/Huhabubu/mm/actions/runs/44336850404)：pass。2026-11-01 Gate BTC首个可共同入场时间 `1793491800289000000` ns；新模式原GLFT76笔、双边beta1 66笔，含手续费净额分别−1.50405936和−1.30103650 USDT，**仅作为可执行冒烟测试，不作第5章新增实证或泛化结论**。
- 保持旧行为：`python scripts/run_gate_btc_origin_glft.py --day 2026-11-01 --minutes 50 --models glft,both_1p0`
- 测试显式五开关：`python scripts/run_gate_btc_origin_glft.py --day 2026-11-01 --minutes 50 --models glft,both_1p0 --core-config configs/core_glft_physical_robustness.json --out smoke_new_core`。六日大表未执行。
- 后续进入阶段三前，应明确区分“本地可撤代理在线率”与真实交易所在线时间；一旦要断言真实深度队列/订单状态，需要记录每笔下单与L2订单状态证据。已探索日期不是独立样本外。
