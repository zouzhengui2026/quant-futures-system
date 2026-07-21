# Codex Development Guide

## Role

Codex is the implementation engineer.

Codex executes approved architecture decisions. Codex does not define trading philosophy or risk policy.

---

# Development Workflow

```
Issue
 ↓
Design
 ↓
Implementation
 ↓
Testing
 ↓
Review
 ↓
Merge
```

---

# Codex Responsibilities

Allowed:

- Implement modules
- Write tests
- Refactor code
- Improve performance
- Fix bugs
- Improve documentation

Not allowed:

- Changing architecture without approval
- Removing risk checks
- Adding live trading shortcuts
- Increasing leverage logic
- Bypassing validation

---

# Trading System Rules

Strategies cannot:

- Directly send orders
- Control leverage
- Override risk limits
- Modify account safety parameters

All trading decisions must pass through:

```
Decision Engine
Portfolio Engine
Margin Engine
Liquidation Risk Engine
Risk Engine
Execution Engine
```

---

# Code Quality Requirements

Every feature requires:

- Unit tests
- Clear documentation
- Logging
- Error handling
- Configuration validation

---

# Production Restrictions

Before live trading:

- Backtest completed
- Walk forward completed
- Paper trading completed
- Risk review completed
- Deployment checklist completed
