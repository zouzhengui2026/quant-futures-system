# System Design

## Design Philosophy

The system follows:

```
Observation
    ↓
Understanding
    ↓
Opportunity
    ↓
Decision
    ↓
Risk Control
    ↓
Execution
    ↓
Attribution
    ↓
Evolution
```

## Core Layers

## 1. Market Data Layer

Responsible for:

- Price data
- Funding rate
- Open interest
- Volume
- Liquidation information
- Exchange metadata

No strategy logic exists here.

## 2. Observation Layer

Implements the principle of observing before acting.

Outputs:

- Market regime
- Trend state
- Volatility state
- Liquidity state
- Crowding state
- Flow state

## 3. Alpha Layer

Each alpha module must represent a clear market mechanism.

Examples:

- Trend continuation
- Relative strength
- Funding imbalance
- Liquidation recovery
- Volatility expansion

## 4. Decision Layer

Responsible for:

- Opportunity scoring
- Conviction estimation
- Position sizing request
- Action selection

It does not directly execute trades.

## 5. Risk Layer

The final authority before execution.

Checks:

- Margin
- Liquidation distance
- Portfolio exposure
- Drawdown limits
- Market abnormality

## 6. Execution Layer

Responsible for:

- Order placement
- Order tracking
- Fill handling
- Exchange communication

Execution must be independent from strategy logic.

## 7. Attribution Layer

Every trade must produce an explanation:

- Why entered
- Which alpha contributed
- What risk existed
- Why exited
- What was learned

## Engineering Principles

- Modular architecture
- Testable components
- Event driven design
- Complete audit trail
- Safe failure modes
- Human understandable decisions
