# Equal-Volume G0 pretrain on 2026-05-01, validate 2026-06-01

选择半价差只使用五月训练日G0成交数与80%事前目标，六月不参与参数选择。
Selected static extra half spread: 0.05 bps
June G0 matched fills 254, pre-existing c3_cancel fills 238
Fill mismatch 6.72%, fee mismatch 6.73%
Strict <2% match: False
若实际未达到<2%，只能称为训练日固定的近似无Binance方向基准，不能称严格等量控制。
所有经济结果均保留2bps手续费，不把无费收益作为主要成果。
