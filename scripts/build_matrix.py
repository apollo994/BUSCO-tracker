#!/usr/bin/env python3
"""
Build a GitHub Actions matrix for batch BUSCO processing.

Reads annotations.tsv, BUSCO.tsv, and error_log.tsv, then computes the
pending annotation IDs in priority order:
  1. Never run  — in annotations but not in BUSCO or error_log
  2. Failed     — in error_log but not yet in BUSCO

Emits GitHub Actions outputs:
  - matrix        JSON array of chunk indices [0, 1, ..., N-1]
  - chunk_count   total number of chunks (N)
  - pending_count total number of pending annotations

Usage:
    python build_matrix.py <annotations_tsv> <busco_tsv> <error_log_tsv> [--max-per-job N]

Writes to $GITHUB_OUTPUT if the environment variable is set, otherwise prints
to stdout (useful for local testing).
"""
import sys
import json
import os
import argparse
import logging
from pathlib import Path

from utils import load_ids, compute_pending_ids

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


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
    parser.add_argument('busco_tsv',       help='Path to BUSCO.tsv (successful results)')
    parser.add_argument('error_log_tsv',   help='Path to error_log.tsv (failed runs)')
    parser.add_argument('--max-chunks', type=int, default=256,
                        help='Maximum number of matrix chunks (default: 256)')
    parser.add_argument('--max-per-job', type=int, default=None,
                        help='Maximum annotations per job; limits total processed per trigger')
    args = parser.parse_args()

    all_ids     = load_ids(args.annotations_tsv)
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

    success_ids = load_ids(args.busco_tsv)
    error_ids   = load_ids(args.error_log_tsv)

    pending_ids = compute_pending_ids(all_ids, success_ids, error_ids)
    never_run   = all_ids - success_ids - error_ids
    failed      = error_ids - success_ids

    logger.info(f"Total annotations : {len(all_ids)}")
    logger.info(f"Successful        : {len(success_ids)}")
    logger.info(f"Never run         : {len(never_run)}")
    logger.info(f"Failed (retry)    : {len(failed)}")
    logger.info(f"Pending (total)   : {len(pending_ids)}")

    if not pending_ids:
        logger.info("No pending annotations — matrix will be empty, busco jobs will be skipped")
        n_chunks = 0
    else:
        if args.max_per_job:
            import math
            # Only schedule what can be processed this trigger (256 * max_per_job)
            to_process = min(args.max_chunks * args.max_per_job, len(pending_ids))
            n_chunks = min(args.max_chunks, math.ceil(to_process / args.max_per_job))
            logger.info(f"Annotations this trigger: {to_process} "
                        f"({len(pending_ids) - to_process} deferred to next run)")
        else:
            n_chunks = min(args.max_chunks, len(pending_ids))

    logger.info(f"Chunks to create  : {n_chunks}")

    matrix = json.dumps(list(range(n_chunks)))

    write_output('matrix',        matrix)
    write_output('chunk_count',   str(n_chunks))
    write_output('pending_count', str(len(pending_ids)))


if __name__ == '__main__':
    main()
