"""Support functions for the extraction_motoristas pipeline."""
from __future__ import annotations

import os

from . import statics


def raw_path(ingest_date: str) -> str:
    """Build the partitioned raw path for a given ingestion date.

    Args:
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        The raw layer path for that partition.
    """
    return f"{statics.RAW_DIR}/ingest_date={ingest_date}"


def partition_processed(path: str) -> bool:
    """Check whether a partition was already written successfully.

    Args:
        path: Path to the partition directory.

    Returns:
        True if a `_SUCCESS` marker exists under `path`.
    """
    return os.path.exists(os.path.join(path, "_SUCCESS"))
