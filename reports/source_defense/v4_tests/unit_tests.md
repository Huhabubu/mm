# V4 focused deterministic unit test proof

Testing revision: \`b337ede3f28d30a32f0ae6630b6765ba335c795c\`
Total: 33 tests, all passed.

|Test module|Passed cases|Status|
|---|---:|---|
|test_experiment_source_defense_emergency_cancel.py|5|PASS|
|test_experiment_source_defense_trade_flow.py|3|PASS|
|test_experiment_source_defense_signal.py|12|PASS|
|test_experiment_source_defense_robust_fit.py|6|PASS|
|test_experiment_source_defense_step1_diagnostic.py|3|PASS|
|test_experiment_source_defense_pilot.py|4|PASS|

Emergency 2ms regression uses the SAME engine cancel helper and a controllable fake HBT order state. Official exchange-ack delay remains simulated in full HBT run.
