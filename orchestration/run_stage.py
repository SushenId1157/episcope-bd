"""
EpiScope - run one pipeline stage from the command line (ENG-1).

The Makefile calls this rather than a notebook, so a clean clone reproduces
the lake with no interactive steps. Each stage is separately invocable
because a failure part-way through a 40-minute rebuild should not require
repeating the stages that already succeeded.
"""
from __future__ import annotations

import argparse
import os
import sys


def build_spark(driver_memory: str = "8g"):
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("EpiScope")
            .master("local[*]")
            .config("spark.driver.memory", driver_memory)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "32")
            .config("spark.sql.session.timeZone", "Asia/Dhaka")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate())


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--stage", required=True,
                   choices=["generator", "bronze", "silver", "gold"])
    p.add_argument("--sf", type=float, default=1.0)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--data", default="./episcope_data")
    p.add_argument("--public", default="./episcope_public")
    a = p.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)

    import orchestration.run_pipeline as rp

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Shipping the project to the executors is required for any UDF that
    # imports project code; without it a pandas UDF fails with
    # ModuleNotFoundError even though the driver imported it fine.
    _ship_modules(spark, root)

    all_stages = ["generator", "bronze", "silver", "gold"]
    skip = tuple(s for s in all_stages if s != a.stage)

    rec = rp.run_all(spark, root, a.public, a.data,
                     scale_factor=a.sf, seed=a.seed, skip=skip)
    for k, v in rec["stage_times_sec"].items():
        print(f"  {k:<20}{v:>9.1f}s")
    print(f"  {'total':<20}{rec['stage_times_sec'].get('total', 0):>9.1f}s")
    return 0


def _ship_modules(spark, root: str) -> None:
    import zipfile
    zp = os.path.join(root, ".episcope_modules.zip")
    if os.path.exists(zp):
        os.remove(zp)
    with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as z:
        for pkg in ["analytics", "pipeline", "generator", "orchestration",
                    "serving"]:
            base = os.path.join(root, pkg)
            if not os.path.isdir(base):
                continue
            for dp, dirs, files in os.walk(base):
                dirs[:] = [d for d in dirs if d != "__pycache__"]
                for f in files:
                    if f.endswith(".py"):
                        full = os.path.join(dp, f)
                        z.write(full, os.path.relpath(full, root))
    spark.sparkContext.addPyFile(zp)


if __name__ == "__main__":
    raise SystemExit(main())
