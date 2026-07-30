# Quant Futures System — Product v0.1 release candidate

A deterministic, dependency-free local research product for replaying perpetual-futures
bars through a reusable strategy, simulated market fills, account bookkeeping, risk
checks, and self-contained reports. **Real-money execution is unavailable.** The product
contains no credentials, private exchange access, deposits, withdrawals, or order routing.

## Architecture

```text
strict UTC CSV -> future-blind strategy -> simulated fill -> portfolio/account
                                                        -> risk -> journal/report
```

## Install and five-minute quick start

Python 3.11 or newer is required.

```bash
python -m pip install -e .
quant-futures validate-config examples/btc_ma.yaml
quant-futures backtest --config examples/btc_ma.yaml
quant-futures paper --config examples/btc_ma_paper.yaml --replay examples/data/btc_usdt_1h.csv
```

`paper --replay` is a **finite historical replay preview**, not paced or
restartable paper trading. Interrupted-run continuation is not supported.

### Paper Runtime control plane (Phase 14, checkpoint 1)

Checkpoint 1 also installs the scriptable lifecycle control surface:

```bash
quant-futures paper start --config examples/btc_ma_paper.yaml --replay examples/data/btc_usdt_1h.csv --pace 1s
quant-futures paper status RUN_DIRECTORY
quant-futures paper pause RUN_DIRECTORY
quant-futures paper resume RUN_DIRECTORY
quant-futures paper stop RUN_DIRECTORY
quant-futures paper recover RUN_DIRECTORY
quant-futures paper audit RUN_DIRECTORY
```

The runtime now also provides a durable `transitions.journal` foundation. Its versioned,
length-prefixed canonical-JSON envelope records deterministic journal and product transition
identities, stage, event type, effective market time, input cursor, payload, and SHA-256 lineage.
Startup validates the committed prefix in one streaming pass without retaining its history;
steady-state appends use constant-sized tail metadata. Under the run lock, only an incomplete
final header or payload is truncated and fsynced. Completed-frame corruption, invalid schema,
reordering, duplication, modification, and oversized frames fail closed. `paper audit` validates
this authority (a missing or empty journal is valid during Checkpoint 2) and projects its sequence
and tail digest. Market-event consumption, checkpoints, and state restoration remain subsequent
checkpoints. In particular,
`recover` is legal only for a persisted `FAILED_RECOVERABLE`
lifecycle and currently validates lifecycle authority—it does not yet restore trading state.
Invalid or repeated transitions fail closed with exit code 2, while an audit mismatch exits 3.

`lifecycle.jsonl` is the checkpoint-one lifecycle authority. `status.json` is explicitly a
disposable, non-authoritative projection rebuilt from that log. Mutating commands acquire an
OS-backed, non-blocking `.paper-runtime.lock`; a concurrent writer is rejected rather than
waiting or racing. This control plane does not introduce accounting or risk state and does not
claim exactly-once processing or restart safety.

Each command prints its deterministic run ID, directory, return, drawdown, trade count,
and final equity. Remove or select a different `output_directory` before repeating an
identical run: collision refusal protects existing evidence.

## Configuration reference

`mode` is `backtest` or `paper`; `data` selects the path, source, symbol, timeframe and
explicit column schema. `starting_equity`, `fill_timing` (`next_open` or
`current_close`), `costs` (commission/slippage bps), `risk`, output directory,
and random seed have explicit defaults. Relative paths resolve against the YAML file.

## Strategies

Both modes use the same `Strategy` protocol and current-only `StrategyContext`. Included
strategies are `moving_average_crossover`, `channel_breakout`, `hold`, and `flat`.
Implement `target(context)` and publish a stable name/version to add a strategy; strategies
return desired position and never mutate accounting state.

## Artifacts and audit

Every run includes `manifest.json`, `config.resolved.yaml`, `summary.json`, `equity.csv`,
`positions.csv`, `trades.csv`, `risk_breaches.csv`, `events.jsonl`, `report.html`, an atomic
`checkpoint.json`, and `status.json`. Run `quant-futures audit RUN_DIRECTORY` to compare the
journal, exact schemas/digests, and reconstructed final state. Audit accepts only completed
runs; it is not an interrupted-run recovery API.

The included data is **synthetic sample data**, created solely for deterministic software
demonstration; it is not exchange history or investment advice.

## Assumptions and limitations

Market orders fill at current close or next bar open with fixed costs. The runtime is a
single-account, single-process simulator. It does not model order books, leverage, margin,
liquidation, exchange latency, or live feeds. CSV is supported by the dependency-free RC;
Parquet requires a future optional adapter. Funding applies only on rows with an explicit,
non-blank funding value and charges the position carried into that timestamp; absent or
blank values mean no funding event. The legacy configured funding rate is not a schedule.

## Troubleshooting

Configuration and CSV errors identify the invalid field or row and exit with code 2.
All timestamps must explicitly be UTC and OHLCV values must be finite and valid. An existing
run directory is never overwritten. Audit mismatch or checkpoint corruption fails closed.

## Deferred beyond Product v0.1

Restartable/paced paper trading, Parquet adapters, multi-symbol orchestration, and production
event sourcing are explicitly out of scope. Live or real-money execution is unavailable.
