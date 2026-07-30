"""Console interface with stable exit codes."""

from __future__ import annotations

import argparse
import sys

from .config import ConfigError, load_config
from .orchestrator import audit, run


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="quant-futures")
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("validate-config", "backtest", "paper"):
        command = commands.add_parser(name)
        command.add_argument("--config", required=True)
        if name == "paper":
            command.add_argument("--replay", help="finite historical replay preview (not restartable)")
    audit_command = commands.add_parser("audit")
    audit_command.add_argument("run_directory")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "audit":
            valid = audit(args.run_directory)
            print("audit: exact match" if valid else "audit: mismatch")
            return 0 if valid else 3
        config = load_config(args.config)
        if args.command == "validate-config":
            print(f"valid {config.mode} configuration: {args.config}")
            return 0
        if args.command != config.mode:
            raise ConfigError(f"command {args.command} requires mode: {args.command}")
        identifier, directory, summary = run(config, getattr(args, "replay", None))
        print(f"run_id: {identifier}\noutput_directory: {directory}\ntotal_return: {summary['total_return']:.8f}\nmaximum_drawdown: {summary['maximum_drawdown']:.8f}\ntrade_count: {summary['trade_count']}\nfinal_equity: {summary['final_equity']:.8f}")
        return 0
    except (ConfigError, ValueError, OSError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
