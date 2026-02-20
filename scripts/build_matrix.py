#!/usr/bin/env python3
"""
Build a GitHub Actions matrix for batch BUSCO processing.

Reads annotations.tsv and log.tsv, computes the set of pending annotation IDs
(present in annotations but not yet logged), and emits GitHub Actions outputs:
  - matrix        JSON array of chunk indices [0, 1, ..., N-1]
  - chunk_count   total number of chunks (N)
  - pending_count total number of pending annotations

Usage:
    python build_matrix.py <annotations_tsv> <log_tsv> [--max-chunks N]

Writes to $GITHUB_OUTPUT if the environment variable is set, otherwise prints
to stdout (useful for local testing).
"""
import sys
import csv
import json
import os
import argparse
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def load_ids(tsv_path, column='annotation_id'):
    """Return set of values from a TSV column. Returns empty set if file missing."""
    p = Path(tsv_path)
    if not p.exists():
        logger.info(f"{tsv_path} not found — treating as empty")
        return set()
    ids = set()
    with open(p, newline='') as f:
        first_line = f.readline()
        if not first_line:
            return ids
        f.seek(0)
        # Detect whether file has a header by checking if first field looks like a column name
        has_header = first_line.split('\t')[0].strip() == column
        if has_header:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                if row.get(column):
                    ids.add(row[column])
        else:
            # No header: annotation_id is always the first column
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                if row and row[0].strip():
                    ids.add(row[0].strip())
    return ids


def write_output(key, value):
    """Write a key=value pair to $GITHUB_OUTPUT or stdout."""
    github_output = os.environ.get('GITHUB_OUTPUT')
    line = f"{key}={value}"
    if github_output:
        with open(github_output, 'a') as f:
            f.write(line + '\n')
    else:
        print(line)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('annotations_tsv', help='Path to annotations.tsv')
    parser.add_argument('log_tsv',         help='Path to log.tsv')
    parser.add_argument('--max-chunks', type=int, default=256,
                        help='Maximum number of matrix chunks (default: 256)')
    args = parser.parse_args()

    all_ids      = load_ids(args.annotations_tsv)
    if not all_ids:
        p = Path(args.annotations_tsv)
        if not p.exists():
            logger.error(f"annotations.tsv not found: {args.annotations_tsv}")
            sys.exit(1)
        logger.warning("annotations.tsv is empty — nothing to process")
        write_output('matrix',        '[]')
        write_output('chunk_count',   '0')
        write_output('pending_count', '0')
        return
    logged_ids   = load_ids(args.log_tsv)
    pending_ids  = all_ids - logged_ids

    logger.info(f"Total annotations : {len(all_ids)}")
    logger.info(f"Already logged    : {len(logged_ids)}")
    logger.info(f"Pending           : {len(pending_ids)}")

    if not pending_ids:
        logger.info("No pending annotations — matrix will be empty, busco jobs will be skipped")
        n_chunks = 0
    else:
        n_chunks = min(args.max_chunks, len(pending_ids))

    logger.info(f"Chunks to create  : {n_chunks}")

    matrix = json.dumps(list(range(n_chunks)))

    write_output('matrix',        matrix)
    write_output('chunk_count',   str(n_chunks))
    write_output('pending_count', str(len(pending_ids)))


if __name__ == '__main__':
    main()
