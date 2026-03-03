#!/usr/bin/env python3
"""
Remove stale annotation entries from BUSCO.tsv and error_log.tsv.

An annotation is stale when its annotation_id is no longer present in
annotations.tsv (e.g. it was removed from the AnnoTrEive API).

Usage:
    python cleanup_stale.py <annotations_tsv> <busco_tsv> <error_log_tsv>
"""
import sys
import argparse
import logging
from pathlib import Path

from utils import load_ids

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def filter_tsv(tsv_path, valid_ids):
    """Remove rows whose annotation_id is not in valid_ids. Returns removed count."""
    p = Path(tsv_path)
    if not p.exists():
        logger.info(f"{tsv_path} not found — skipping")
        return 0

    kept = []
    removed = 0
    with open(p) as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            annotation_id = stripped.split('\t', 1)[0].strip()
            # Always keep the header row
            if annotation_id == 'annotation_id':
                kept.append(line)
                continue
            if annotation_id in valid_ids:
                kept.append(line)
            else:
                removed += 1
                logger.info(f"  Removing stale entry: {annotation_id}")

    with open(p, 'w') as f:
        f.writelines(kept)

    return removed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('annotations_tsv', help='Path to annotations.tsv')
    parser.add_argument('busco_tsv',       help='Path to BUSCO.tsv (successful results)')
    parser.add_argument('error_log_tsv',   help='Path to error_log.tsv (failed runs)')
    args = parser.parse_args()

    valid_ids = load_ids(args.annotations_tsv)
    if not valid_ids:
        logger.error("No annotation IDs loaded from annotations.tsv — aborting to avoid data loss")
        sys.exit(1)

    logger.info(f"Loaded {len(valid_ids)} valid annotation IDs from {args.annotations_tsv}")

    removed_busco = filter_tsv(args.busco_tsv, valid_ids)
    removed_errors = filter_tsv(args.error_log_tsv, valid_ids)

    logger.info(f"Cleanup complete: removed {removed_busco} rows from BUSCO.tsv, "
                f"{removed_errors} rows from error_log.tsv")

    if removed_busco == 0 and removed_errors == 0:
        logger.info("No stale entries found — files unchanged")


if __name__ == '__main__':
    main()
