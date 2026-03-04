#!/usr/bin/env python3
"""
Aggregate per-job BUSCO result fragments into the shared TSV files.

Usage:
    python aggregate_results.py <artifacts_dir> <busco_tsv> <retry_tsv>

Each run-busco-analysis job uploads up to two files:
    result_<annotation_id>.tsv   -- one BUSCO row (header + data) on success
    log_<annotation_id>.tsv      -- one retry row (header + data) on failure

This script scans <artifacts_dir> recursively for those fragments and appends
new rows, skipping any already present.
  - BUSCO.tsv    dedup key: annotation_id       (one success row per annotation)
  - .retry.log   dedup key: (annotation_id, run_at)  (full history of failures)
"""
import sys
import csv
import logging
from pathlib import Path

from utils import BUSCO_HEADER, RETRY_HEADER

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


def load_existing_retry_entries(tsv_path):
    """Return set of (annotation_id, run_at) tuples already in .retry.log."""
    p = Path(tsv_path)
    if not p.exists():
        return set()
    entries = set()
    with open(p, newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            if row.get('annotation_id') and row.get('run_at'):
                entries.add((row['annotation_id'], row['run_at']))
    return entries


def ensure_header(tsv_path, header):
    """Write header if the file does not exist yet."""
    p = Path(tsv_path)
    if not p.exists():
        with open(p, 'w', newline='') as f:
            csv.writer(f, delimiter='\t', lineterminator='\n').writerow(header)


def append_rows(tsv_path, rows):
    """Append a list of dicts to a TSV file (no header written)."""
    if not rows:
        return
    with open(tsv_path, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys(), delimiter='\t', lineterminator='\n')
        writer.writerows(rows)


def read_fragment(fragment_path, expected_header):
    """Read a fragment TSV. Returns list of row dicts matching expected_header."""
    rows = []
    with open(fragment_path, newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            if set(expected_header).issubset(row.keys()):
                rows.append({k: row[k] for k in expected_header})
    return rows


def main():
    if len(sys.argv) != 4:
        print("Usage: python aggregate_results.py <artifacts_dir> <busco_tsv> <retry_tsv>")
        sys.exit(1)

    artifacts_dir = Path(sys.argv[1])
    busco_tsv     = sys.argv[2]
    retry_tsv     = sys.argv[3]

    if not artifacts_dir.is_dir():
        logger.error(f"Artifacts directory not found: {artifacts_dir}")
        sys.exit(1)

    existing_busco_ids   = load_existing_ids(busco_tsv)
    existing_retry_entries = load_existing_retry_entries(retry_tsv)
    logger.info(f"Existing BUSCO rows   : {len(existing_busco_ids)}")
    logger.info(f"Existing retry rows   : {len(existing_retry_entries)}")

    ensure_header(busco_tsv,   BUSCO_HEADER)
    ensure_header(retry_tsv,   RETRY_HEADER)

    busco_new = []
    retry_new = []

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
        rows = read_fragment(frag, RETRY_HEADER)
        for row in rows:
            key = (row['annotation_id'], row['run_at'])
            if key not in existing_retry_entries:
                retry_new.append(row)
                existing_retry_entries.add(key)
                logger.info(f"  + retry: {row['annotation_id']} @ {row['run_at']}")

    append_rows(busco_tsv,   busco_new)
    append_rows(retry_tsv,   retry_new)

    logger.info(f"Appended {len(busco_new)} BUSCO rows and {len(retry_new)} retry rows.")


if __name__ == "__main__":
    main()
