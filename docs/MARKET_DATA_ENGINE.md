# Phase 2 Market Data Engine

The market data engine is a read-only collection layer.  It is responsible for
normalizing exchange or replay observations, validating them, retaining the
latest value per source/symbol/data kind, and publishing events to the existing
in-process `EventBus`.

## Supported data kinds

- OHLCV
- Funding rate
- Open interest
- Mark price and index price
- Liquidations
- Instrument metadata

Adapters implement `MarketDataSource.read()` and yield `MarketDataRecord`
objects.  They must not create orders, access execution code, or contain
strategy logic.  `MarketDataCollector` rejects records older than the accepted
timestamp for the same source, symbol, and data kind, emits
`market_data.rejected`, and does not overwrite the retained latest value. Accepted observations emit
`market_data.received`.

This phase intentionally provides no exchange-specific network client and no
durable historical database.  Those are infrastructure implementations behind
the same read-only source and repository boundaries.
