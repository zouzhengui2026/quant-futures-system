"""Immutable account-aware portfolio risk audit records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from math import fsum, isfinite
from numbers import Real

from quant_futures.account.models import AccountSnapshot, PositionValuation
from quant_futures.core.exceptions import DomainValidationError
from quant_futures.portfolio.models import PositionSide, ZERO_TOLERANCE


def _finite(name: str, value: object) -> None:
    if not isinstance(value, Real) or isinstance(value, bool) or not isfinite(value):
        raise DomainValidationError(f"{name} must be a finite real number")


def _positive(name: str, value: object) -> None:
    _finite(name, value)
    if value <= 0:
        raise DomainValidationError(f"{name} must be positive")


def _zero(value: float) -> float:
    return 0.0 if abs(value) <= ZERO_TOLERANCE else value


class RiskLimitCode(str, Enum):
    NON_POSITIVE_EQUITY = "non_positive_equity"
    MAX_GROSS_NOTIONAL = "max_gross_notional"
    MAX_ABS_NET_NOTIONAL = "max_abs_net_notional"
    MAX_POSITION_NOTIONAL = "max_position_notional"
    MAX_CONCENTRATION = "max_concentration"
    MAX_GROSS_EXPOSURE_MULTIPLE = "max_gross_exposure_multiple"


class PortfolioRiskOutcome(str, Enum):
    HEALTHY = "healthy"
    BREACHED = "breached"


@dataclass(frozen=True, slots=True)
class PositionExposure:
    position_valuation: PositionValuation
    signed_notional: float
    position_notional: float

    @property
    def valuation(self) -> PositionValuation:
        """Concise alias retained for consumers of the exposure graph."""
        return self.position_valuation

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        valuation = self.position_valuation
        if not isinstance(valuation, PositionValuation):
            raise DomainValidationError("position_valuation must be a PositionValuation")
        valuation.validate()
        position = valuation.position
        if position.side is PositionSide.FLAT:
            expected_signed = 0.0
        else:
            expected_signed = _zero(position.signed_quantity * valuation.mark_price)
        expected_absolute = _zero(abs(expected_signed))
        _finite("signed_notional", self.signed_notional)
        _finite("position_notional", self.position_notional)
        if self.signed_notional != expected_signed:
            raise DomainValidationError("signed_notional does not match the valuation")
        if self.position_notional != expected_absolute:
            raise DomainValidationError("position_notional does not match signed_notional")


@dataclass(frozen=True, slots=True)
class PortfolioRiskLimits:
    require_positive_equity: bool
    max_gross_notional: float
    max_abs_net_notional: float
    max_position_notional: float
    max_concentration_ratio: float
    max_gross_exposure_multiple: float

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.require_positive_equity, bool):
            raise DomainValidationError("require_positive_equity must be a bool")
        for name in (
            "max_gross_notional", "max_abs_net_notional",
            "max_position_notional", "max_gross_exposure_multiple",
        ):
            _positive(name, getattr(self, name))
        _positive("max_concentration_ratio", self.max_concentration_ratio)
        if self.max_concentration_ratio > 1:
            raise DomainValidationError("max_concentration_ratio must be at most one")


@dataclass(frozen=True, slots=True)
class RiskLimitBreach:
    code: RiskLimitCode
    actual: float
    limit: float | None
    source: str | None = None
    symbol: str | None = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.code, RiskLimitCode):
            raise DomainValidationError("code must be a RiskLimitCode")
        _finite("actual", self.actual)
        if self.code is RiskLimitCode.NON_POSITIVE_EQUITY:
            if self.limit is not None:
                raise DomainValidationError("non-positive-equity breach has no configured limit")
            if self.actual > 0:
                raise DomainValidationError("non-positive-equity breach requires non-positive equity")
        else:
            _positive("limit", self.limit)
            if self.actual <= self.limit:
                raise DomainValidationError("maximum-limit breach requires an exceedance")
        position_specific = self.code is RiskLimitCode.MAX_POSITION_NOTIONAL
        if position_specific:
            for name in ("source", "symbol"):
                value = getattr(self, name)
                if not isinstance(value, str) or not value.strip():
                    raise DomainValidationError(f"{name} is required for a position breach")
        elif self.source is not None or self.symbol is not None:
            raise DomainValidationError("portfolio breaches must not have position attribution")


def build_portfolio_risk_breaches(
    account_snapshot: AccountSnapshot,
    position_exposures: tuple[PositionExposure, ...],
    gross_notional: float,
    net_notional: float,
    concentration_ratio: float,
    gross_exposure_multiple: float | None,
    limits: PortfolioRiskLimits,
) -> tuple[RiskLimitBreach, ...]:
    """Build the one canonical, deterministically ordered limit decision."""
    breaches: list[RiskLimitBreach] = []
    if limits.require_positive_equity and account_snapshot.equity <= 0:
        breaches.append(RiskLimitBreach(
            RiskLimitCode.NON_POSITIVE_EQUITY, account_snapshot.equity, None))
    if gross_notional > limits.max_gross_notional:
        breaches.append(RiskLimitBreach(
            RiskLimitCode.MAX_GROSS_NOTIONAL, gross_notional, limits.max_gross_notional))
    absolute_net = abs(net_notional)
    if absolute_net > limits.max_abs_net_notional:
        breaches.append(RiskLimitBreach(
            RiskLimitCode.MAX_ABS_NET_NOTIONAL, absolute_net,
            limits.max_abs_net_notional))
    for exposure in position_exposures:
        if exposure.position_notional > limits.max_position_notional:
            position = exposure.position_valuation.position
            breaches.append(RiskLimitBreach(
                RiskLimitCode.MAX_POSITION_NOTIONAL, exposure.position_notional,
                limits.max_position_notional, position.source, position.symbol))
    if concentration_ratio > limits.max_concentration_ratio:
        breaches.append(RiskLimitBreach(
            RiskLimitCode.MAX_CONCENTRATION, concentration_ratio,
            limits.max_concentration_ratio))
    if (gross_exposure_multiple is not None and
            gross_exposure_multiple > limits.max_gross_exposure_multiple):
        breaches.append(RiskLimitBreach(
            RiskLimitCode.MAX_GROSS_EXPOSURE_MULTIPLE, gross_exposure_multiple,
            limits.max_gross_exposure_multiple))
    return tuple(breaches)


@dataclass(frozen=True, slots=True)
class PortfolioRiskSnapshot:
    account_snapshot: AccountSnapshot
    position_exposures: tuple[PositionExposure, ...]
    long_notional: float
    short_notional: float
    gross_notional: float
    net_notional: float
    largest_position_notional: float
    concentration_ratio: float
    gross_exposure_multiple: float | None
    limits: PortfolioRiskLimits
    breaches: tuple[RiskLimitBreach, ...]
    outcome: PortfolioRiskOutcome
    evaluated_at: datetime

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.account_snapshot, AccountSnapshot):
            raise DomainValidationError("account_snapshot must be an AccountSnapshot")
        self.account_snapshot.validate()
        if not isinstance(self.position_exposures, tuple):
            raise DomainValidationError("position_exposures must be a tuple")
        if len(self.position_exposures) != len(self.account_snapshot.valuations):
            raise DomainValidationError("exposures must exactly cover account valuations")
        for exposure, valuation in zip(self.position_exposures, self.account_snapshot.valuations):
            if not isinstance(exposure, PositionExposure):
                raise DomainValidationError("position_exposures contains an invalid value")
            exposure.validate()
            if exposure.position_valuation is not valuation:
                raise DomainValidationError("exposure must retain exact valuation identity")
        longs = [max(e.signed_notional, 0.0) for e in self.position_exposures]
        shorts = [max(-e.signed_notional, 0.0) for e in self.position_exposures]
        expected_long, expected_short = _zero(fsum(longs)), _zero(fsum(shorts))
        expected_gross = _zero(fsum((expected_long, expected_short)))
        expected_net = _zero(fsum((expected_long, -expected_short)))
        expected_largest = max((e.position_notional for e in self.position_exposures), default=0.0)
        expected_concentration = 0.0 if expected_gross == 0 else expected_largest / expected_gross
        equity = self.account_snapshot.equity
        expected_multiple = None if equity <= 0 else expected_gross / equity
        expected = (expected_long, expected_short, expected_gross, expected_net,
                    expected_largest, expected_concentration)
        names = ("long_notional", "short_notional", "gross_notional", "net_notional",
                 "largest_position_notional", "concentration_ratio")
        for name, value, wanted in zip(names, (getattr(self, n) for n in names), expected):
            _finite(name, value)
            if value != wanted:
                raise DomainValidationError(f"{name} does not match position exposures")
        if expected_multiple is None:
            if self.gross_exposure_multiple is not None:
                raise DomainValidationError("gross exposure multiple requires positive equity")
        else:
            _finite("gross_exposure_multiple", self.gross_exposure_multiple)
            if self.gross_exposure_multiple != expected_multiple:
                raise DomainValidationError("gross_exposure_multiple is incorrect")
        if not isinstance(self.limits, PortfolioRiskLimits):
            raise DomainValidationError("limits must be PortfolioRiskLimits")
        self.limits.validate()
        if not isinstance(self.breaches, tuple):
            raise DomainValidationError("breaches must be a tuple")
        for breach in self.breaches:
            if not isinstance(breach, RiskLimitBreach):
                raise DomainValidationError("breaches contains an invalid value")
            breach.validate()
        expected_breaches = build_portfolio_risk_breaches(
            self.account_snapshot, self.position_exposures, expected_gross, expected_net,
            expected_concentration, expected_multiple, self.limits)
        if self.breaches != expected_breaches:
            raise DomainValidationError("breaches do not match the canonical limit decision")
        if not isinstance(self.outcome, PortfolioRiskOutcome):
            raise DomainValidationError("outcome must be PortfolioRiskOutcome")
        wanted_outcome = PortfolioRiskOutcome.BREACHED if self.breaches else PortfolioRiskOutcome.HEALTHY
        if self.outcome is not wanted_outcome:
            raise DomainValidationError("outcome must be derived from breaches")
        if self.evaluated_at != self.account_snapshot.valued_at:
            raise DomainValidationError("evaluated_at must equal account valued_at")
