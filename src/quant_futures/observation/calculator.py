from quant_futures.observation.features import MarketFeatures


class FeatureCalculator:
    """Interface for converting market data into structural features.

    Concrete implementations will consume candles, funding, open interest,
    and liquidity data in later phases.
    """

    def calculate(self, market_data) -> MarketFeatures:
        raise NotImplementedError
