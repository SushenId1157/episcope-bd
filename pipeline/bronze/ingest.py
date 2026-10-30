"""
EpiScope - bronze layer ingestion (ING-1, DQ-3).

Bronze holds every source exactly as it arrived. Nothing is cleaned, corrected
or dropped here: invalid ages, orphan facility keys and duplicate encounter ids
all pass through untouched, because bronze is the record of what was actually
received. Cleaning happens in silver, and keeping the two apart is what makes
the lineage in DQ-3 answerable: for any gold figure we can name the batches it
came from and go back to the raw rows.

Every bronze table carries the same ingestion metadata:
  _source_id      which of S1-S6 this is
  _batch_id       one id per load, time-ordered
  _source_file    where the row came from
  _file_checksum  SHA-256 of the source file, so a re-delivered file is
                  recognisable even if its name or timestamp changed
  _load_timestamp when it was ingested
  _ingest_mode    full or incremental
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, functions as F


def make_batch_id(source_id: str, mode: str = "full") -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{source_id}_{mode}_{stamp}"


def checksum_path(path: str, max_files: int = 64) -> str:
    """
    SHA-256 over a file, or over a directory's file names, sizes and contents.

    For a Parquet directory the per-file digests are combined rather than the
    bytes concatenated, so the result is stable regardless of read order and
    cheap enough to compute on a large table.
    """
    h = hashlib.sha256()
    if os.path.isfile(path):
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()

    parts = []
    for dp, _, files in os.walk(path):
        for f in sorted(files):
            if f.startswith("."):
                continue
            fp = os.path.join(dp, f)
            rel = os.path.relpath(fp, path)
            parts.append((rel, os.path.getsize(fp)))
    parts.sort()
    for rel, size in parts[:max_files]:
        h.update(f"{rel}:{size}".encode())
    h.update(f"n_files={len(parts)}".encode())
    return h.hexdigest()


def add_ingestion_metadata(df: DataFrame, source_id: str, batch_id: str,
                           source_file: str, checksum: str,
                           mode: str = "full") -> DataFrame:
    """Attach the ING-1 metadata columns. Source columns are left untouched."""
    return (df
            .withColumn("_source_id", F.lit(source_id))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_source_file", F.lit(source_file))
            .withColumn("_file_checksum", F.lit(checksum))
            .withColumn("_load_timestamp", F.current_timestamp())
            .withColumn("_ingest_mode", F.lit(mode)))


def write_bronze(df: DataFrame, bronze_root: str, table: str,
                 partition_by: list[str] | None = None,
                 mode: str = "overwrite") -> dict:
    """Write one bronze table and return a manifest entry describing the load."""
    out = os.path.join(bronze_root, table)
    w = df.write.mode(mode).option("compression", "snappy")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.parquet(out)

    size = sum(os.path.getsize(os.path.join(dp, f))
               for dp, _, fs in os.walk(out) for f in fs)
    return {"table": table, "path": out, "size_bytes": size,
            "partitioned_by": partition_by or []}


def record_manifest(entries: list[dict], bronze_root: str) -> str:
    """
    Append this load's entries to the bronze manifest.

    The manifest is the index the lineage walk in DQ-3 starts from: given a
    batch id on a gold row, it names the source, the file and the checksum
    that produced it.
    """
    path = os.path.join(bronze_root, "_manifest.json")
    existing = []
    if os.path.exists(path):
        try:
            existing = json.load(open(path))
        except Exception:
            existing = []
    existing.extend(entries)
    os.makedirs(bronze_root, exist_ok=True)
    with open(path, "w") as fh:
        json.dump(existing, fh, indent=2)
    return path


def ingest_source(spark: SparkSession, source_id: str, reader, source_path: str,
                  bronze_root: str, table: str,
                  partition_by: list[str] | None = None,
                  mode: str = "full") -> dict:
    """
    Ingest one source end to end: read it, stamp it, write it, record it.

    `reader` is a callable taking the path and returning a DataFrame, so the
    same function handles CSV, JSON and Parquet sources without branching on
    format here.
    """
    batch_id = make_batch_id(source_id, mode)
    checksum = checksum_path(source_path)
    df = reader(source_path)
    rows_in = df.count()

    stamped = add_ingestion_metadata(df, source_id, batch_id,
                                     os.path.basename(source_path.rstrip("/")),
                                     checksum, mode)
    info = write_bronze(stamped, bronze_root, table, partition_by)
    info.update({"source_id": source_id, "batch_id": batch_id,
                 "source_path": source_path, "checksum": checksum,
                 "rows": rows_in, "ingest_mode": mode,
                 "loaded_at_utc": datetime.now(timezone.utc).isoformat()})
    return info
