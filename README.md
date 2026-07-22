# Quant Futures System

Institutional-grade cryptocurrency perpetual futures quantitative trading system.

## Philosophy

This project is designed around systematic market observation, opportunity detection, alpha extraction, disciplined execution and continuous improvement.

Core principles:

- Observe before acting.
- Identify structural opportunities, not random signals.
- Understand the source of profit before trading.
- Respect leverage, liquidity and risk boundaries.
- Execute through strict rules.

## Architecture Philosophy

```text
Observation
    ↓
Market Regime
    ↓
Flow Analysis
    ↓
Alpha Sources
    ↓
Opportunity Evaluation
    ↓
Portfolio & Risk
    ↓
Execution
    ↓
Attribution & Improvement
```

## Development Status

Phase 0: Architecture and trading principles.

No live trading capability exists at this stage.

## Runtime Communication

`EventBus` is an internal, in-process runtime communication mechanism for
decoupling system layers. It transports events between components but does not
connect to exchanges, submit orders, or perform trading execution.
