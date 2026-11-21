# Tokyo L2 incremental QA state

Generated read-only audit reports are committed here by the scheduled GitHub workflow.
Each result is keyed by an immutable cloud manifest path and QA_VERSION; `ok` and `blocked` outcomes are both retained and skipped on subsequent runs. Failed downloads are retried. This branch does not contain raw market data or credentials.
