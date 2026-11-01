"""
EpiScope - assemble the ENG-2 scalability table from recorded runs.

Reads whatever pipeline_sf*.json files exist and prints the table the report
carries. It states what was measured and on what hardware; it does not
interpolate a scale that was never run.
"""
from __future__ import annotations

import glob
import json
import os
import sys


def main() -> int:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    exp = os.path.join(root, "docs", "experiments")
    files = sorted(glob.glob(os.path.join(exp, "pipeline_sf*.json")))
    if not files:
        print("No pipeline runs recorded. Run `make scaling` first.")
        return 1

    runs = []
    for f in files:
        try:
            d = json.load(open(f))
            if "stage_times_sec" in d:
                runs.append((d.get("scale_factor", "?"), d))
        except Exception:
            continue
    runs.sort(key=lambda t: float(t[0]) if t[0] != "?" else 0)

    stages = ["weekly_counts", "encounters", "stock", "bronze", "silver", "gold"]
    print(f"{'stage':<18}" + "".join(f"{'SF=' + str(sf):>12}"
                                     for sf, _ in runs))
    print("-" * (18 + 12 * len(runs)))
    for s in stages:
        row = f"{s:<18}"
        for _, d in runs:
            v = d["stage_times_sec"].get(s)
            row += f"{v:>11.1f}s" if v is not None else f"{'-':>12}"
        print(row)
    print("-" * (18 + 12 * len(runs)))
    row = f"{'TOTAL':<18}"
    for _, d in runs:
        t = d.get("total_sec") or sum(
            v for k, v in d["stage_times_sec"].items() if k != "total")
        row += f"{t/60:>10.1f}m"
    print(row)

    last = runs[-1][1]
    hw = last.get("hardware", {})
    print(f"\nhardware: {hw.get('cores', '?')} cores, "
          f"{hw.get('ram_gb', '?')} GB, {hw.get('platform', '?')}")
    print(f"ENG-2 requires SF=1 within 45 minutes.")
    for sf, d in runs:
        t = d.get("total_min") or (d.get("total_sec", 0) / 60)
        if str(sf) in ("1.0", "1"):
            print(f"  SF=1 measured at {t:.1f} min: "
                  f"{'within budget' if t <= 45 else 'OVER BUDGET'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
