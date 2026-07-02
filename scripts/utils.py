"""Path helpers shared by every pipeline."""
from __future__ import annotations

import os


def raw_path(raw_dir: str, ingest_date: str) -> str:
    """Build the partitioned raw path for a given ingestion date.

    Args:
        raw_dir: Base directory of the dataset's raw layer.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        The raw layer path for that partition.
    """
    return f"{raw_dir}/ingest_date={ingest_date}"


def staging_path(staging_dir: str, ingest_date: str) -> str:
    """Build the partitioned staging path for a given ingestion date.

    Args:
        staging_dir: Base directory of the dataset's staging layer.
        ingest_date: Ingestion date in `YYYY-MM-DD` format.

    Returns:
        The staging layer path for that partition.
    """
    return f"{staging_dir}/ingest_date={ingest_date}"


def partition_processed(path: str) -> bool:
    """Check whether a partition was already written successfully.

    Args:
        path: Path to the partition directory.

    Returns:
        True if a `_SUCCESS` marker exists under `path`.
    """
    return os.path.exists(os.path.join(path, "_SUCCESS"))
