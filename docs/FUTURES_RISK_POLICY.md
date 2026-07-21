# Futures Risk Policy

## Purpose

Protect capital while allowing systematic opportunity capture.

## Non-Negotiable Rules

The system must never:

- Use martingale sizing.
- Add to losing positions.
- Allow strategies to directly place orders.
- Allow strategies to decide leverage.
- Bypass risk checks.
- Enter live trading without validation.

## Trading Requirements

Every order must pass:

1. Data quality check.
2. Market state check.
3. Opportunity evaluation.
4. Margin check.
5. Liquidation distance check.
6. Portfolio exposure check.
7. Risk engine approval.

## Position Management

Allowed:

- Scaling into profitable positions.
- Reducing risk when conditions deteriorate.
- Protecting accumulated profits.

Forbidden:

- Emotional averaging down.
- Unlimited leverage expansion.
- Manual override of safety systems.

## Deployment Rule

Development → Backtest → Paper Trading → Small Live → Scale.

No shortcuts.
