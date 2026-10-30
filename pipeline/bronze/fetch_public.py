"""
EpiScope - public data acquisition helpers (sources S1, S3).

Every download is recorded with ingestion metadata required by ING-1:
source URL, load timestamp, batch id and SHA-256 file checksum.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone


def sha256_file(path: str) -> str:
    """Return the SHA-256 checksum of a file, read in chunks to bound memory."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def make_batch_id(source_id: str) -> str:
    """Deterministic-looking, time-ordered batch id, e.g. S1_20261029T174500Z."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{source_id}_{stamp}"


def http_get(url: str, timeout: int = 120, retries: int = 3,
             backoff: float = 2.0) -> bytes:
    """GET a URL with simple exponential backoff. Raises the last error."""
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "EpiScope-CSE4345-coursework"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as err:
            last_err = err
            if attempt < retries - 1:
                time.sleep(backoff ** attempt)
    raise RuntimeError(f"GET failed after {retries} attempts: {url}") from last_err


def download_to_bronze(url: str, dest_path: str, source_id: str,
                       description: str, licence: str,
                       overwrite: bool = False) -> dict:
    """
    Download `url` to `dest_path` and write a sidecar *.meta.json holding the
    ingestion metadata demanded by ING-1. Returns the metadata dict.

    If the file already exists and overwrite is False, the download is skipped
    but the checksum is recomputed, so re-running is cheap and idempotent.
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    reused = os.path.exists(dest_path) and not overwrite

    if not reused:
        payload = http_get(url)
        with open(dest_path, "wb") as fh:
            fh.write(payload)

    meta = {
        "source_id": source_id,
        "description": description,
        "source_url": url,
        "licence": licence,
        "local_path": dest_path,
        "batch_id": make_batch_id(source_id),
        "load_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "file_bytes": os.path.getsize(dest_path),
        "sha256": sha256_file(dest_path),
        "reused_existing_file": reused,
    }
    with open(dest_path + ".meta.json", "w") as fh:
        json.dump(meta, fh, indent=2)
    return meta
