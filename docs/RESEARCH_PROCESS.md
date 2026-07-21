# Quantitative Research Process

## Purpose

Define the mandatory lifecycle for every trading strategy before it can reach production.

A strategy is not accepted because of a good backtest. It must demonstrate a repeatable market mechanism, robustness, and operational safety.

## Strategy Lifecycle

```
Idea
  ↓
Economic Hypothesis
  ↓
Market Mechanism Definition
  ↓
Data Research
  ↓
Backtest
  ↓
Walk Forward Validation
  ↓
Paper Trading
  ↓
Small Capital Validation
  ↓
Performance Attribution
  ↓
Production Approval
```

## Strategy Documentation Requirements

Every strategy must document:

- Alpha source
- Market mechanism
- Expected edge
- Applicable market regimes
- Invalid conditions
- Risk profile
- Capacity limits
- Expected holding period
- Transaction cost assumptions

## Backtesting Standards

Backtests must include:

- Fees
- Funding costs
- Slippage assumptions
- Position sizing rules
- Liquidation simulation
- Missing data handling
- Outlier analysis

## Validation Requirements

Required validation:

- In-sample testing
- Out-of-sample testing
- Walk-forward testing
- Parameter sensitivity testing
- Regime analysis
- Drawdown analysis

## Paper Trading Requirements

Before live trading:

- Minimum observation period completed
- Execution behavior verified
- Order state transitions verified
- Risk controls verified
- Attribution reports generated

## Core Principle

A strategy is not valuable because it predicts the market.

A strategy is valuable because it has a measurable edge, survives uncertainty, and can be executed repeatedly.