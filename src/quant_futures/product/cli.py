"""Console interface with stable exit codes."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path

from quant_futures.paper_runtime import (LifecycleError, LifecycleState, OperationalError,
                                         RunLockError)
from quant_futures.paper_runtime import control as paper_control

from .config import ConfigError, load_config
from .orchestrator import audit, run
from .data import load_bars
from .strategy import build_strategy


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="quant-futures")
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("validate-config", "backtest"):
        command = commands.add_parser(name)
        command.add_argument("--config", required=True)
    paper = commands.add_parser("paper")
    paper.add_argument("--config", help="legacy finite replay-preview configuration")
    paper.add_argument("--replay", help="legacy finite historical replay preview")
    paper_commands = paper.add_subparsers(dest="paper_command")
    start = paper_commands.add_parser("start")
    start.add_argument("--config", required=True)
    start.add_argument("--replay", required=True)
    start.add_argument("--pace", default="0s")
    for name in ("status", "pause", "resume", "stop", "recover", "audit"):
        control = paper_commands.add_parser(name)
        control.add_argument("run_directory")
    audit_command = commands.add_parser("audit")
    audit_command.add_argument("run_directory")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "paper" and args.paper_command:
            if args.paper_command == "start":
                config = load_config(args.config)
                if config.mode != "paper":
                    raise ConfigError("paper start requires mode: paper")
                if not args.pace.endswith("s") or not args.pace[:-1].replace(".", "", 1).isdigit():
                    raise ConfigError("--pace must be a non-negative duration in seconds, for example 1s")
                if not Path(args.replay).is_file():
                    raise ConfigError(f"replay input does not exist: {args.replay}")
                directory = paper_control.start(Path(config.output_directory) / "paper-runtime")
                print(f"run_directory: {directory}", flush=True)
                replay_path = Path(args.replay).resolve()
                config_path = Path(args.config).resolve()
                pace_seconds = float(args.pace[:-1])
                paper_control.write_runtime_metadata(directory, config_path, replay_path,
                                                     pace_seconds)
                effective = replace(config, data=replace(config.data, path=str(replay_path)))
                bars, fingerprint = load_bars(replay_path, effective.data.schema,
                    start=effective.data.start, end=effective.data.end,
                    timeframe=effective.data.timeframe)
                from quant_futures.paper_runtime import (PaperRuntime,
                    PaperTransitionCoordinator, RuntimeConsumerLease, StopFlag,
                    TransitionJournal)
                # Claim lifetime ownership before constructing Product
                # authorities. Process death releases this OS lease.
                consumer_lease = RuntimeConsumerLease(directory).acquire()
                run_id = paper_control.project_status(directory)["run_id"]
                coordinator = PaperTransitionCoordinator(
                    str(run_id), effective,
                    build_strategy(effective.strategy.name, effective.strategy.parameters),
                    TransitionJournal(directory), data_fingerprint=f"sha256:{fingerprint}")
                stop_flag = StopFlag(); stop_flag.install()
                PaperRuntime(coordinator, pace_seconds=pace_seconds,
                             stop_flag=stop_flag,
                             consumer_lease=consumer_lease).run(bars)
                return 0
            if args.paper_command == "status":
                print(json.dumps(paper_control.project_status(args.run_directory), sort_keys=True))
                return 0
            if args.paper_command == "pause":
                if (Path(args.run_directory) / "runtime.json").exists():
                    from quant_futures.paper_runtime import OperationalRequests
                    OperationalRequests(args.run_directory).request("pause")
                else:
                    paper_control.transition(args.run_directory, LifecycleState.PAUSED, "pause requested")
            elif args.paper_command == "resume":
                if (Path(args.run_directory) / "runtime.json").exists():
                    status = paper_control.project_status(args.run_directory)
                    if status["lifecycle"] == LifecycleState.PAUSED.value:
                        paper_control.resume_runtime(args.run_directory,
                                                     install_signals=True)
                    else:
                        from quant_futures.paper_runtime import OperationalRequests
                        OperationalRequests(args.run_directory).request("resume")
                else:
                    paper_control.transition(args.run_directory, LifecycleState.RUNNING, "resume requested")
            elif args.paper_command == "stop":
                if (Path(args.run_directory) / "runtime.json").exists():
                    paper_control.request_stop(args.run_directory)
                else:
                    paper_control.stop(args.run_directory)
            elif args.paper_command == "recover":
                paper_control.recover(args.run_directory)
                paper_control.continue_runtime(args.run_directory, install_signals=True)
            else:
                valid = paper_control.audit(args.run_directory)
                print("paper runtime audit: exact match" if valid else "paper runtime audit: mismatch")
                return 0 if valid else 3
            print(f"lifecycle: {paper_control.project_status(args.run_directory)['lifecycle']}")
            return 0
        if args.command == "audit":
            valid = audit(args.run_directory)
            print("audit: exact match" if valid else "audit: mismatch")
            return 0 if valid else 3
        if args.command == "paper" and not args.config:
            raise ConfigError("paper requires a control command or --config for the legacy replay preview")
        config = load_config(args.config)
        if args.command == "validate-config":
            print(f"valid {config.mode} configuration: {args.config}")
            return 0
        if args.command != config.mode:
            raise ConfigError(f"command {args.command} requires mode: {args.command}")
        identifier, directory, summary = run(config, getattr(args, "replay", None))
        print(f"run_id: {identifier}\noutput_directory: {directory}\ntotal_return: {summary['total_return']:.8f}\nmaximum_drawdown: {summary['maximum_drawdown']:.8f}\ntrade_count: {summary['trade_count']}\nfinal_equity: {summary['final_equity']:.8f}")
        return 0
    except (ConfigError, LifecycleError, OperationalError, RunLockError, ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
