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

### Restart-safe Paper Runtime operator guide

The installed console script provides the finite, single-symbol Paper Runtime:

```bash
quant-futures paper start --config examples/btc_ma_paper.yaml --replay examples/data/btc_usdt_1h.csv --pace 1s
quant-futures paper status RUN_DIRECTORY
quant-futures paper pause RUN_DIRECTORY
quant-futures paper resume RUN_DIRECTORY
quant-futures paper stop RUN_DIRECTORY
quant-futures paper recover RUN_DIRECTORY
quant-futures paper audit RUN_DIRECTORY
```

`start` prints the run directory immediately, then owns and consumes the replay. Use a nonzero
pace when another process must issue controls. `pause` takes effect only after the current input
has committed, checkpoints that boundary, and makes the owner exit. `resume` reopens that same
directory and consumes only the remaining inputs. A paused `stop` records
`STOPPING -> COMPLETED`; repeating `stop` is a successful no-op. `SIGINT` and `SIGTERM` merely set
a process-local flag: the runtime performs the same boundary-safe terminal stop as an operator
request. Terminal runs reject resume and recovery.

Only one consumer may own a run. A separate OS-released lifetime lease makes a competing
start/continue/recover fail before Product mutation. Status distinguishes a live RUNNING owner,
relinquished PAUSED/terminal runs, and an ownerless RUNNING run (`stalled`). `recover` rejects a
live owner; for a stalled or partial run it validates all authority, completes at most one legal
transition suffix, publishes the matching checkpoint, and continues the remaining replay.

#### Run-directory authority

| Artifact | Role |
| --- | --- |
| `lifecycle.jsonl` | Append-only lifecycle authority and legal operational lineage. |
| `transitions.journal` | Protected framed Product-stage authority with deterministic IDs and SHA-256 chain. |
| `checkpoint.json` | Atomic, bounded canonical Product state at the last committed transition. |
| `recovery-attempts.jsonl` | Hash-chained recovery invocations, committed back into the checkpoint. |
| `runtime.json` | Immutable config/replay paths, content hashes, and pacing needed to reopen. |
| `control-request.json` | Monotonic boundary request; operational, not trading authority. |
| `status.json` | Disposable projection only. It may be rebuilt and must exactly match authority. |
| `.paper-runtime.lock`, `.paper-runtime-consumer.lock` | Transaction lock and lifetime lease; never Product state. |

Each Product input is journaled in causal stages. The cursor advances only at durable
`transition_committed`, after which the bounded checkpoint is atomically replaced and directory
fsynced. Recovery recomputes already-written stages through the same Product strategy, execution,
portfolio, account, and risk engines, verifies their protected payloads, and appends only the
missing suffix. This gives exactly-once Product authority for the supported finite replay model,
including current-close and carried next-open fills, fixed commission/slippage, explicit-row
funding, cash flow, peak equity, and drawdown. It does not promise exactly-once effects in an
external exchange or filesystem durability beyond the host OS/storage guarantees.

`paper audit` is read-only and fail-closed. It validates lifecycle, journal framing/digests,
checkpoint/recovery commitments, replay/config content identity, status equality, and absence of
protocol temporary artifacts. Audit never invents or repairs Product state. Recovery may remove
only a provably incomplete final journal frame and exact-pattern orphan checkpoint temporaries;
completed-frame corruption, a malformed or ambiguous suffix, stale authority, or identity change
requires preserving the directory for investigation. Do not hand-edit authority files.

Deterministic command exit codes are: **0** success (including idempotent controls), **2** invalid
input/state, contention, terminal rejection, or other operational failure, and **3** audit
mismatch. Commands emit concise errors on stderr and scriptable JSON/status text on stdout.

Troubleshooting order: run `paper status`, verify no other owner is live, copy the directory,
then run `paper audit`. Use `recover` only for a stalled/recoverable run. A changed config or CSV,
corrupt checkpoint/journal/recovery chain, forged protocol temporary, or terminal run fails closed.
Never delete a recovery record or replace a replay file in place.

The runtime is a local deterministic simulator: no exchange credentials, private APIs,
WebSockets, live feeds, real orders, multi-symbol coordination, leverage, margin, liquidation,
order-book liquidity, or crash-atomic external side effects are provided. It is not a daemon,
HA service, trading recommendation, or production exchange connector.

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
