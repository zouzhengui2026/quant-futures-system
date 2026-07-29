# Quant Futures System — v1.0 release candidate

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

Each command prints its deterministic run ID, directory, return, drawdown, trade count,
and final equity. Remove or select a different `output_directory` before repeating an
identical run: collision refusal protects existing evidence.

## Configuration reference

`mode` is `backtest` or `paper`; `data` selects the path, source, symbol, timeframe and
explicit column schema. `starting_equity`, `fill_timing` (`next_open` or
`current_close`), `costs` (commission/slippage bps and funding), `risk`, output directory,
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
journal digest and reconstruct the exact last state.

The included data is **synthetic sample data**, created solely for deterministic software
demonstration; it is not exchange history or investment advice.

## Assumptions and limitations

Market orders fill at current close or next bar open with fixed costs. The runtime is a
single-account, single-process simulator. It does not model order books, leverage, margin,
liquidation, exchange latency, or live feeds. CSV is supported by the dependency-free RC;
Parquet requires a future optional adapter.

## Troubleshooting

Configuration and CSV errors identify the invalid field or row and exit with code 2.
All timestamps must explicitly be UTC and OHLCV values must be finite and valid. An existing
run directory is never overwritten. Audit mismatch or checkpoint corruption fails closed.

## Roadmap after v1.0 RC

Optional Parquet adapters, multi-symbol portfolio orchestration, richer trade attribution,
paced external normalized feeds, and stronger crash/concurrency testing are planned. Live
or real-money execution is deliberately not on the RC roadmap.
