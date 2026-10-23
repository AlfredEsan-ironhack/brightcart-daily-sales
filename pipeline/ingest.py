"""Step 1 - copy the received files into the raw layer, unchanged.

Rules:
* Raw is a byte-for-byte copy. Nothing is cleaned here.
* Rerunning with the *same* file is a no-op (idempotent).
* Rerunning with a *different* file for a date that is already ingested fails
  loudly. Raw is our evidence of what we received; it is never silently
  overwritten. ``force=True`` (Airflow run conf ``{"force_reingest": true}``)
  is a deliberate operator decision: the old raw file is moved to
  raw/_superseded/... and the new one is ingested.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .config import ENTITIES, Paths
from .io_utils import copy_file, count_data_rows, read_json, sha256, write_json


class RawConflictError(RuntimeError):
    """A different file arrived for a date whose raw data already exists."""


def _as_bool(value) -> bool:
    """Airflow templates render to text ("True"/"False"), so accept both."""
    return value if isinstance(value, bool) else str(value).strip().lower() in {"1", "true", "yes"}


def _archive_raw(p: Paths, entity: str) -> Path:
    """Keep the superseded raw file as evidence instead of overwriting it."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = p.root / "raw" / "_superseded" / p.partition / stamp / f"{entity}.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    p.raw(entity).replace(target)
    return target


def ingest_raw(business_date: str, root: Path | None = None, force=False) -> dict:
    force = _as_bool(force)
    p = Paths.for_date(business_date, root)
    manifest = read_json(p.raw_manifest) if p.raw_manifest.exists() else {"business_date": business_date, "files": {}}

    for entity in ENTITIES:
        src = p.incoming(entity)
        if not src.exists():
            raise FileNotFoundError(f"Expected source file not found: {src}")

        dst = p.raw(entity)
        src_hash = sha256(src)

        if dst.exists():
            if sha256(dst) == src_hash:
                print(f"[ingest] {entity}: already ingested with the same checksum, skipping")
                continue
            if not force:
                raise RawConflictError(
                    f"{entity} for {business_date} was already ingested with a different checksum. "
                    f"Investigate the re-delivery; if it should replace raw, rerun with force_reingest=true."
                )
            archived = _archive_raw(p, entity)
            print(f"[ingest] {entity}: force re-ingest, previous raw archived to {archived}")

        copy_file(src, dst)
        if sha256(dst) != src_hash:  # verify the copy, do not assume it
            raise IOError(f"Checksum mismatch after copying {src} -> {dst}")

        manifest["files"][entity] = {
            "source_file": str(src),
            "raw_file": str(dst),
            "sha256": src_hash,
            "bytes": dst.stat().st_size,
            "rows": count_data_rows(dst),
            "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        print(f"[ingest] {entity}: {manifest['files'][entity]['rows']} rows copied to {dst}")

    write_json(manifest, p.raw_manifest)
    return manifest
