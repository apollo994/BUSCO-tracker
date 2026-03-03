#!/usr/bin/env python3
"""
Triage error_log entries: give up on annotations that have failed more than once.

Reads error_log.tsv, groups rows by annotation_id, and:
  - already in BUSCO.tsv → remove from error_log.tsv (succeeded on retry)
  - count == 1  → keep in error_log.tsv  (will be retried once more)
  - count  > 1  → append to giveup.log and remove from error_log.tsv

Usage:
    python triage_errors.py <error_log_tsv> <giveup_tsv> <busco_tsv>
"""
import csv
import logging
import sys
from collections import defaultdict
from pathlib import Path

from utils import GIVEUP_HEADER, ERROR_LOG_HEADER, load_ids

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_existing_giveup_entries(tsv_path):
    """Return set of (annotation_id, run_at) already in giveup.log."""
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


def main():
    if len(sys.argv) != 4:
        print("Usage: python triage_errors.py <error_log_tsv> <giveup_tsv> <busco_tsv>")
        sys.exit(1)

    error_log_tsv = sys.argv[1]
    giveup_tsv    = sys.argv[2]
    busco_tsv     = sys.argv[3]

    error_path = Path(error_log_tsv)
    if not error_path.exists():
        logger.info(f"{error_log_tsv} not found — nothing to triage")
        return

    # Read all error_log rows, grouped by annotation_id
    groups = defaultdict(list)
    with open(error_path, newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            aid = row.get('annotation_id', '').strip()
            if aid:
                groups[aid].append(row)

    if not groups:
        logger.info("error_log.tsv is empty — nothing to triage")
        return

    # Remove entries for annotations that have since succeeded
    success_ids = load_ids(busco_tsv)
    resolved = {aid for aid in groups if aid in success_ids}
    for aid in resolved:
        logger.info(f"  Resolved (now in BUSCO.tsv): {aid}")
        del groups[aid]

    # Partition remaining: keep (count == 1) vs giveup (count > 1)
    keep_rows   = []
    giveup_rows = []
    for aid, rows in groups.items():
        if len(rows) > 1:
            giveup_rows.extend(rows)
            logger.info(f"  Give up: {aid} ({len(rows)} failures)")
        else:
            keep_rows.extend(rows)

    logger.info(f"Triage: {len(resolved)} resolved (succeeded on retry), "
                f"{len(keep_rows)} rows kept, "
                f"{len(giveup_rows)} rows moved to giveup.log "
                f"({len(groups) - len(keep_rows)} annotations given up)")

    # Append new giveup rows (dedup against existing entries)
    existing_giveup = load_existing_giveup_entries(giveup_tsv)
    ensure_header(giveup_tsv, GIVEUP_HEADER)

    new_giveup = [r for r in giveup_rows
                  if (r['annotation_id'], r['run_at']) not in existing_giveup]

    if new_giveup:
        with open(giveup_tsv, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=GIVEUP_HEADER,
                                    delimiter='\t', lineterminator='\n')
            writer.writerows(new_giveup)
        logger.info(f"Appended {len(new_giveup)} new rows to {giveup_tsv}")

    # Rewrite error_log.tsv with only the kept rows
    with open(error_path, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow(ERROR_LOG_HEADER)
        for row in keep_rows:
            writer.writerow([row[col] for col in ERROR_LOG_HEADER])

    logger.info("Triage complete")


if __name__ == '__main__':
    main()
