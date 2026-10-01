from __future__ import annotations

import subprocess
import sys
import re

MINIMUM = 90


def main() -> int:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stdout + result.stderr)
        return result.returncode
    node_ids = sum("::test_" in line for line in result.stdout.splitlines())
    grouped = sum(
        int(match.group(1))
        for line in result.stdout.splitlines()
        if (match := re.match(r"^tests/.+: (\d+)$", line))
    )
    count = node_ids or grouped
    print(f"Collected risk-oriented test cases: {count} (required: {MINIMUM})")
    if count < MINIMUM:
        print("Test count is below the portfolio acceptance threshold.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
