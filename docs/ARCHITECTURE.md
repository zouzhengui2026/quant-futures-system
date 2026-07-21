# Quant Futures System Architecture

## 1. Design Philosophy

This system is designed as an institutional-style crypto perpetual futures quantitative trading platform.

Core principle:

```
Observe → Understand → Evaluate → Act → Control → Learn
```

The system follows the Yinfu framework:

- Observation: understand market structure.
- Timing: identify opportunity windows.
- Alpha: define the source of profit.
- Action: execute controlled decisions.
- Discipline: enforce risk boundaries.

---

# 2. High Level Architecture

```
Market Data Layer
        ↓
Observation Layer
        ↓
Regime & Flow Analysis Layer
        ↓
Alpha Research Layer
        ↓
Decision Engine
        ↓
Portfolio & Risk Layer
        ↓
Execution Layer
        ↓
Attribution & Learning Layer
```

---

# 3. Core Components

## Data Layer

Responsible for:

- OHLCV
- Funding rate
- Open interest
- Mark price
- Index price
- Volume
- Liquidation data
- Exchange metadata

Requirements:

- Timestamp accuracy
- Data validation
- Missing data detection
- Historical storage

---

## Observation Layer

The system's eyes.

Responsibilities:

- Market state recognition
- Trend analysis
- Volatility analysis
- Liquidity analysis
- Crowding analysis

Output:

```
MarketState
TrendState
VolatilityState
LiquidityState
CrowdingState
```

---

## Regime Engine

Determines market environment.

Initial states:

```
TREND_UP
TREND_DOWN
RANGE
PANIC
RECOVERY
RISK_OFF
```

Regime changes must be gradual and evidence based.

---

## Flow State Engine

Analyzes market energy flow:

- Capital flow
- Leverage flow
- Liquidation pressure
- Funding pressure
- Open interest changes

---

## Alpha Engine

Every strategy must declare:

- Profit source
- Market mechanism
- Suitable regime
- Failure conditions
- Capacity

No unexplained alpha is allowed.

---

## Decision Engine

Combines:

- Market state
- Timing
- Alpha quality
- Risk
- Portfolio impact

Output actions:

```
NO_TRADE
PROBE
OPEN
ADD
REDUCE
CLOSE
DELEVERAGE
```

---

## Risk Layer

The system survival layer.

Includes:

- Margin engine
- Liquidation risk engine
- Portfolio risk engine
- Kill switch

Risk always overrides opportunity.

---

## Execution Layer

Responsible for:

- Order creation
- Order lifecycle
- Exchange communication
- Fill tracking
- Reconciliation

Strategies never directly access execution.

---

## Attribution Layer

Every trade must answer:

- Why entered?
- Why exited?
- Which alpha generated profit?
- Which risk caused loss?
- What should improve?

---

# 4. Engineering Principles

1. Separation of concerns.
2. Risk before execution.
3. Explainable decisions.
4. Complete audit trail.
5. Test before deployment.
6. Paper trading before live trading.
