"""Logging helpers for core runtime components."""

import logging


def get_logger(name: str) -> logging.Logger:
    """Return a named standard-library logger without configuring global logging."""
    return logging.getLogger(name)
