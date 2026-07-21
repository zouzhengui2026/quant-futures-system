# Live Trading Readiness Checklist

## Purpose

No strategy or system component enters live trading without completing this checklist.

## Infrastructure

- VPS stable
- Deployment reproducible
- Monitoring active
- Logs available
- Backup and recovery tested
- Time synchronization verified

## Exchange Integration

- API permissions reviewed
- Withdrawal permissions disabled
- Test orders completed
- Order status synchronization verified
- Position reconciliation verified

## Risk Controls

Required:

- Maximum position limits
- Maximum leverage limits
- Maximum daily loss limit
- Maximum drawdown protection
- Liquidation distance monitoring
- Emergency shutdown mechanism
- Manual kill switch

## Strategy Validation

Required:

- Research document completed
- Backtest completed
- Walk-forward completed
- Paper trading completed
- Attribution report reviewed

## Operational Rules

The system must be able to answer:

- Why was this trade opened?
- Which alpha source generated it?
- What risk checks passed?
- What would force an exit?
- What caused the final result?

## Production Principle

Live trading is not the beginning of research.

Live trading is the final stage after evidence has been accumulated.