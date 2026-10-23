"""Small file helpers.

Writes are *atomic*: we write to a temporary file in the same folder and then
rename it into place. A crash halfway through never leaves a half-written file
that the next step could read as if it were complete.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd


def _atomic_replace(path: Path, write_fn) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    os.close(fd)
    try:
        write_fn(tmp_name)
        os.replace(tmp_name, path)  # rename is atomic on the same filesystem
    except BaseException:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise


def write_csv(df: pd.DataFrame, path: Path) -> None:
    _atomic_replace(Path(path), lambda tmp: df.to_csv(tmp, index=False))


def write_json(obj: Any, path: Path) -> None:
    def _write(tmp: str) -> None:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, indent=2, sort_keys=False, default=str)
            fh.write("\n")

    _atomic_replace(Path(path), _write)


def copy_file(src: Path, dst: Path) -> None:
    data = Path(src).read_bytes()
    _atomic_replace(Path(dst), lambda tmp: Path(tmp).write_bytes(data))


def read_json(path: Path) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def read_csv_as_text(path: Path) -> pd.DataFrame:
    """Read every column as text, exactly as received.

    ``dtype=str`` stops pandas from guessing types (e.g. "007" -> 7) and
    ``keep_default_na=False`` keeps empty cells as "" instead of NaN.
    """
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def count_data_rows(path: Path) -> int:
    return len(read_csv_as_text(path))
