#!/usr/bin/env python3
"""
Aggregate per-job BUSCO result fragments into the shared TSV files.

Usage:
    python aggregate_results.py <artifacts_dir> <busco_tsv> <log_tsv>

Each run-busco-analysis job uploads two files:
    result_<annotation_id>.tsv   -- one BUSCO row (header + data), or header-only on failure
    log_<annotation_id>.tsv      -- one log row (header + data)

This script scans <artifacts_dir> recursively for those fragments,
skips annotation_ids already present in <busco_tsv> or <log_tsv>,
and appends new rows.
"""
import sys
import csv
import logging
import os
import tempfile
from pathlib import Path

from utils import load_failed_ids, BUSCO_HEADER, LOG_HEADER

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_existing_ids(tsv_path):
    """Return set of annotation_ids already in a TSV file."""
    p = Path(tsv_path)
    if not p.exists():
        return set()
    ids = set()
    with open(p, newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            if row.get('annotation_id'):
                ids.add(row['annotation_id'])
    return ids


def remove_ids_from_tsv(tsv_path, ids_to_remove):
    """Rewrite tsv_path keeping only rows whose annotation_id is NOT in ids_to_remove.
    Uses an atomic write (temp file + os.replace) to avoid corruption on failure.
    """
    p = Path(tsv_path)
    if not p.exists() or not ids_to_remove:
        return
    with open(p, newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        header = reader.fieldnames
        rows = [row for row in reader if row.get('annotation_id') not in ids_to_remove]
    tmp_fd, tmp_path = tempfile.mkstemp(dir=p.parent, suffix='.tmp')
    try:
        with os.fdopen(tmp_fd, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=header, delimiter='\t')
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, p)
    except Exception:
        os.unlink(tmp_path)
        raise
    logger.info(f"Removed {len(ids_to_remove)} failed rows from {tsv_path}")


def ensure_header(tsv_path, header):
    """Write header if the file does not exist yet."""
    p = Path(tsv_path)
    if not p.exists():
        with open(p, 'w', newline='') as f:
            csv.writer(f, delimiter='\t').writerow(header)


def append_rows(tsv_path, rows):
    """Append a list of dicts to a TSV file (no header written)."""
    if not rows:
        return
    with open(tsv_path, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys(), delimiter='\t')
        writer.writerows(rows)


def read_fragment(fragment_path, expected_header):
    """
    Read a single-row fragment TSV.
    Returns list of row dicts (usually 0 or 1 items).
    """
    rows = []
    with open(fragment_path, newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            if set(expected_header).issubset(row.keys()):
                rows.append({k: row[k] for k in expected_header})
    return rows


def main():
    if len(sys.argv) not in (4, 5):
        print("Usage: python aggregate_results.py <artifacts_dir> <busco_tsv> <log_tsv> [--retry-failed]")
        sys.exit(1)

    artifacts_dir = Path(sys.argv[1])
    busco_tsv     = sys.argv[2]
    log_tsv       = sys.argv[3]
    retry_failed  = '--retry-failed' in sys.argv

    if not artifacts_dir.is_dir():
        logger.error(f"Artifacts directory not found: {artifacts_dir}")
        sys.exit(1)

    # In retry mode, remove existing failed log (and busco) rows for IDs being
    # retried so they can be replaced with fresh results.
    if retry_failed:
        retried_ids = load_failed_ids(log_tsv)
        logger.info(f"Retry mode: removing {len(retried_ids)} failed rows before aggregating")
        remove_ids_from_tsv(log_tsv, retried_ids)

    # Load IDs already committed to avoid duplicates
    existing_busco_ids = load_existing_ids(busco_tsv)
    existing_log_ids   = load_existing_ids(log_tsv)
    logger.info(f"Existing BUSCO rows: {len(existing_busco_ids)}")
    logger.info(f"Existing log rows:   {len(existing_log_ids)}")

    ensure_header(busco_tsv, BUSCO_HEADER)
    ensure_header(log_tsv,   LOG_HEADER)

    busco_new = []
    log_new   = []

    # Scan all result_*.tsv and log_*.tsv fragments recursively
    result_fragments = sorted(artifacts_dir.rglob("result_*.tsv"))
    log_fragments    = sorted(artifacts_dir.rglob("log_*.tsv"))

    for frag in result_fragments:
        rows = read_fragment(frag, BUSCO_HEADER)
        for row in rows:
            if row['annotation_id'] not in existing_busco_ids:
                busco_new.append(row)
                existing_busco_ids.add(row['annotation_id'])
                logger.info(f"  + BUSCO: {row['annotation_id']}")
            else:
                logger.info(f"  ~ skip (already exists): {row['annotation_id']}")

    for frag in log_fragments:
        rows = read_fragment(frag, LOG_HEADER)
        for row in rows:
            if row['annotation_id'] not in existing_log_ids:
                log_new.append(row)
                existing_log_ids.add(row['annotation_id'])
                logger.info(f"  + log:   {row['annotation_id']}")

    append_rows(busco_tsv, busco_new)
    append_rows(log_tsv,   log_new)

    logger.info(f"Appended {len(busco_new)} BUSCO rows and {len(log_new)} log rows.")


if __name__ == "__main__":
    main()
