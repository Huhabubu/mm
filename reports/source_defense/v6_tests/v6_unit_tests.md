# V6研究集中单元测试

全部通过 32 项；使用HBT 2.4.4。

|模块|数量|
|---|---:|
|test_experiment_source_defense_v6_mechanism.py|8|
|test_experiment_source_defense_emergency_cancel.py|5|
|test_experiment_source_defense_trade_flow.py|4|
|test_experiment_source_defense_signal.py|12|
|test_experiment_source_defense_v5_pepe_coverage.py|3|

测试包含双时钟有符号领先/滞后、+60秒严格等时长时序与未越时窗口、2ms立即危险侧撤单等逻辑；HBT交易执行另有真实回放。
