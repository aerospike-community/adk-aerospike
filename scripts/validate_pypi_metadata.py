#!/usr/bin/env python3
"""Fail if built wheel/sdist metadata contains PyPI-forbidden VCS direct deps."""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path


def _check_metadata(text: str, label: str) -> list[str]:
    errors: list[str] = []
    if "git+" in text or " @ git" in text:
        errors.append(f"{label}: VCS/direct URL dependency in metadata")
    if "Extra: benchmark" in text and "ai-ecosystem-benchmark" in text:
        errors.append(f"{label}: [benchmark] extra still in metadata (use benchmarks/requirements.txt)")
    return errors


def main() -> int:
    dist = Path("dist")
    if not dist.is_dir():
        print("dist/ not found; run python -m build first", file=sys.stderr)
        return 1

    errors: list[str] = []
    for whl in sorted(dist.glob("*.whl")):
        with zipfile.ZipFile(whl) as zf:
            meta_name = next(n for n in zf.namelist() if n.endswith(".dist-info/METADATA"))
            errors.extend(_check_metadata(zf.read(meta_name).decode(), whl.name))

    if errors:
        for err in errors:
            print(err, file=sys.stderr)
        return 1

    print("PyPI metadata OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
