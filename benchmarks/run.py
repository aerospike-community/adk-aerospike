#!/usr/bin/env python3
"""Run adk-aerospike workloads through ai-ecosystem-benchmark.

Install the framework (not on PyPI yet):

    pip install -e ".[benchmark]"
    # or: pip install "git+https://github.com/aerospike-community/ai-ecosystem-benchmark.git"

Usage:

    python benchmarks/run.py --list-profiles
    python benchmarks/run.py --profile smoke
    python benchmarks/run.py --profile sustained --uri "aerospike://host:3000/ns?set_prefix=bench_"
    python benchmarks/run.py --profile smoke --backend redis --uri "redis://127.0.0.1:6379/1"
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_BENCHMARKS_DIR = Path(__file__).resolve().parent
_PROFILES_DIR = _BENCHMARKS_DIR / "profiles"
_REPO_ROOT = _BENCHMARKS_DIR.parent


def _require_framework() -> tuple[Any, Any]:
    try:
        from ai_ecosystem_benchmark import BaseBenchmarkWorkload, BenchmarkRunner
    except ImportError:
        print(
            "ai-ecosystem-benchmark is not installed.\n\n"
            "  pip install -e \".[benchmark]\"\n"
            "  # or\n"
            "  pip install "
            "\"git+https://github.com/aerospike-community/ai-ecosystem-benchmark.git\"\n",
            file=sys.stderr,
        )
        sys.exit(1)
    return BaseBenchmarkWorkload, BenchmarkRunner


def _load_profile(name: str) -> dict[str, Any]:
    path = _PROFILES_DIR / f"{name}.json"
    if not path.is_file():
        known = sorted(p.stem for p in _PROFILES_DIR.glob("*.json"))
        print(f"unknown profile {name!r}; choose from: {', '.join(known)}", file=sys.stderr)
        sys.exit(1)
    return json.loads(path.read_text())


def _list_profiles() -> None:
    for path in sorted(_PROFILES_DIR.glob("*.json")):
        data = json.loads(path.read_text())
        desc = data.get("description", "")
        workload = data.get("workload", "?")
        qps = data.get("queries_per_second", "?")
        print(f"  {path.stem:<16} workload={workload:<16} qps={qps}  {desc}")


def _list_workloads() -> None:
    from benchmarks.workloads import WORKLOAD_TYPES

    for name, cls in sorted(WORKLOAD_TYPES.items()):
        tests = [
            m
            for m in dir(cls)
            if m.startswith("aerospike_") and callable(getattr(cls, m))
        ]
        print(f"  {name}")
        for test in sorted(tests):
            print(f"    - {test}")


def main() -> None:
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--profile", help="JSON profile name under benchmarks/profiles/")
    p.add_argument("--uri", help="override profile connection_string")
    p.add_argument(
        "--backend",
        choices=("aerospike", "redis"),
        default="aerospike",
        help="storage backend to benchmark (default: aerospike)",
    )
    p.add_argument(
        "--results-dir",
        type=Path,
        help="write metrics transcript to <dir>/<profile>-<backend>.txt",
    )
    p.add_argument("--list-profiles", action="store_true")
    p.add_argument("--list-workloads", action="store_true")
    args = p.parse_args()

    if args.list_profiles:
        _list_profiles()
        return
    if args.list_workloads:
        _list_workloads()
        return
    if not args.profile:
        p.error("--profile is required (or use --list-profiles)")

    _, BenchmarkRunner = _require_framework()
    from benchmarks.workloads import build_workload

    profile = _load_profile(args.profile)
    uri = args.uri or profile["connection_string"]
    workload = build_workload(
        profile["workload"],
        uri,
        profile.get("workload_params", {}),
        backend=args.backend,
    )

    runner = BenchmarkRunner(
        queries_per_second=int(profile["queries_per_second"]),
        scheduler_thread_count=int(profile["scheduler_thread_count"]),
        worker_thread_count=int(profile["worker_thread_count"]),
        runtime_per_function=int(profile["runtime_per_function"]),
        workload=workload,
    )

    desc = profile.get("description", "")
    print(f"profile={args.profile}  workload={profile['workload']}")
    if desc:
        print(f"  {desc}")
    print(f"  uri={uri}")
    print(
        f"  qps={runner.queries_per_second}  runtime={runner.runtime_per_function}s  "
        f"schedulers={runner.scheduler_thread_count}  workers={runner.worker_thread_count}"
    )
    if args.backend == "aerospike":
        tests = [t.__name__ for t in workload.get_aerospike_tests()]
    else:
        tests = [t.__name__ for t in workload.get_redis_tests()]
    print(f"  backend={args.backend}  tests: {', '.join(tests)}")
    print()

    runner.run()
    if args.results_dir:
        args.results_dir.mkdir(parents=True, exist_ok=True)
        out_path = args.results_dir / f"{args.profile}-{args.backend}.txt"
        import contextlib
        import io

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            runner.print_metrics()
        transcript = buf.getvalue()
        print(transcript, end="")
        out_path.write_text(transcript)
        print(f"Wrote metrics to {out_path}")
    else:
        runner.print_metrics()


if __name__ == "__main__":
    main()
