# OpenClaw Runbook

## Purpose

OpenClaw is the operations layer for deployment, monitoring and maintenance.

OpenClaw is not a strategy engine.

---

# Responsibilities

OpenClaw may:

- Pull approved code
- Run tests
- Deploy services
- Monitor processes
- Collect logs
- Report failures
- Restart failed services

---

# Restrictions

OpenClaw may not:

- Enable live trading independently
- Increase capital allocation
- Modify risk parameters
- Change strategy logic
- Disable safety checks

---

# Deployment Flow

```
GitHub
 ↓
CI Validation
 ↓
OpenClaw Pull
 ↓
Environment Check
 ↓
Deployment
 ↓
Health Monitoring
```

---

# Production Requirements

Before live deployment:

- Tests passed
- Configuration reviewed
- Secrets verified
- Risk controls enabled
- Emergency shutdown tested

---

# Monitoring

Monitor:

- Service health
- Data freshness
- Exchange connection
- Position state
- Orders
- Margin status
- Errors
- Risk alerts
