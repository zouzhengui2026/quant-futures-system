"""Deterministic baseline execution-intent policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import isfinite
from numbers import Real
from typing import Callable
from uuid import uuid4

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.decision.models import DecisionAction
from quant_futures.domain.order import Order, OrderSide, OrderStatus
from quant_futures.risk.models import RiskAssessment, RiskOutcome

from .models import ExecutionIntent


@dataclass(frozen=True, slots=True)
class FixedQuantityExecutionPolicy:
    """Create a CREATED order with fixed sizing and no external side effects."""

    quantity: float = 1.0
    price: float | None = None
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    order_id_factory: Callable[[], str] = field(default=lambda: str(uuid4()))
    name: str = "fixed_quantity_execution_baseline"

    def __post_init__(self) -> None:
        self._validate_configuration()

    def create_intent(self, risk_assessment: RiskAssessment) -> ExecutionIntent:
        self._validate_configuration()
        if not isinstance(risk_assessment, RiskAssessment):
            raise TypeError("create_intent expects a RiskAssessment")
        risk_assessment.validate()
        if risk_assessment.outcome is not RiskOutcome.APPROVED:
            raise DomainValidationError("only an APPROVED RiskAssessment may create an intent")
        action = risk_assessment.decision_intent.action
        sides = {
            DecisionAction.PROPOSE_LONG: OrderSide.BUY,
            DecisionAction.PROPOSE_SHORT: OrderSide.SELL,
        }
        if action not in sides:
            raise DomainValidationError("only a directional proposal may create an intent")

        created_at = self.clock()
        order_id = self.order_id_factory()
        if not isinstance(order_id, str) or not order_id.strip():
            raise DomainValidationError("order_id_factory must return a non-empty string")
        order = Order(
            order_id=order_id,
            symbol=risk_assessment.symbol,
            side=sides[action],
            quantity=self.quantity,
            price=self.price,
            status=OrderStatus.CREATED,
            created_at=created_at,
        )
        return ExecutionIntent(
            symbol=risk_assessment.symbol,
            source=risk_assessment.source,
            created_at=created_at,
            policy_name=self.name,
            order=order,
            risk_assessment=risk_assessment,
        )

    def _validate_configuration(self) -> None:
        self._positive("quantity", self.quantity)
        if self.price is not None:
            self._positive("price", self.price)
        if not callable(self.clock):
            raise DomainValidationError("clock must be callable")
        if not callable(self.order_id_factory):
            raise DomainValidationError("order_id_factory must be callable")
        if not isinstance(self.name, str) or not self.name.strip():
            raise DomainValidationError("name must be a non-empty string")

    @staticmethod
    def _positive(name: str, value: object) -> None:
        if (
            not isinstance(value, Real)
            or isinstance(value, bool)
            or not isfinite(value)
            or value <= 0
        ):
            raise DomainValidationError(f"{name} must be a finite positive number")
