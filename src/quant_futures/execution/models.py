"""Immutable, auditable execution intentions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.decision.models import DecisionAction
from quant_futures.domain.order import Order, OrderSide, OrderStatus
from quant_futures.risk.models import RiskAssessment, RiskOutcome


@dataclass(frozen=True, slots=True)
class ExecutionIntent:
    """A validated request to create an order record, not to submit it."""

    symbol: str
    source: str
    created_at: datetime
    policy_name: str
    order: Order
    risk_assessment: RiskAssessment

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Fail closed if this intent or any upstream value was tampered with."""
        for name, value in (
            ("symbol", self.symbol),
            ("source", self.source),
            ("policy_name", self.policy_name),
        ):
            if not isinstance(value, str) or not value.strip():
                raise DomainValidationError(f"{name} must be a non-empty string")
        if not isinstance(self.risk_assessment, RiskAssessment):
            raise DomainValidationError("risk_assessment must be a RiskAssessment")
        self.risk_assessment.validate()
        if self.risk_assessment.outcome is not RiskOutcome.APPROVED:
            raise DomainValidationError("risk_assessment must be APPROVED")
        if not isinstance(self.order, Order):
            raise DomainValidationError("order must be an Order")
        self.order.validate()
        if self.order.status is not OrderStatus.CREATED:
            raise DomainValidationError("order status must be CREATED")
        self._validate_time(self.created_at)
        risk = self.risk_assessment
        if self.created_at < risk.assessed_at:
            raise DomainValidationError("created_at must not be earlier than assessed_at")
        if self.order.created_at < risk.assessed_at:
            raise DomainValidationError("order.created_at must not be earlier than assessed_at")
        if self.created_at < self.order.created_at:
            raise DomainValidationError("created_at must not be earlier than order.created_at")
        if self.symbol != risk.symbol or self.source != risk.source:
            raise DomainValidationError("symbol and source must match risk_assessment")
        if self.order.symbol != risk.symbol:
            raise DomainValidationError("order.symbol must match risk_assessment.symbol")
        action = risk.decision_intent.action
        expected_side = {
            DecisionAction.PROPOSE_LONG: OrderSide.BUY,
            DecisionAction.PROPOSE_SHORT: OrderSide.SELL,
        }.get(action)
        if expected_side is None:
            raise DomainValidationError("only a directional proposal may create an execution intent")
        if self.order.side is not expected_side:
            raise DomainValidationError("order.side must match the decision action")

    @staticmethod
    def _validate_time(value: object) -> None:
        if not isinstance(value, datetime):
            raise DomainValidationError("created_at must be a timezone-aware datetime")
        try:
            offset = value.utcoffset()
        except Exception as exc:
            raise DomainValidationError("created_at must be a timezone-aware datetime") from exc
        if value.tzinfo is None or offset is None:
            raise DomainValidationError("created_at must be a timezone-aware datetime")
