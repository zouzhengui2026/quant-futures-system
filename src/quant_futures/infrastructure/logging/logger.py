"""Minimal structured logger abstraction.

Future versions will add JSON logging, correlation ids and runtime tracing.
"""

import logging


def get_logger(name: str) -> logging.Logger:
    """Return application logger."""
    return logging.getLogger(name)
