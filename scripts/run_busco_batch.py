#!/usr/bin/env python3
"""
Run a strided slice of pending BUSCO analyses for one matrix job.

Usage:
    python run_busco_batch.py <annotations_tsv> <log_tsv> \
        <chunk_index> <chunk_count> <output_dir>

Slicing: pending_sorted[chunk_index::chunk_count]
  e.g.  chunk 0 of 4 on [a,b,c,d,e,f,g,h] → [a,e]
        chunk 1 of 4                        → [b,f]
        chunk 2 of 4                        → [c,g]
        chunk 3 of 4                        → [d,h]

Per-annotation failures are recorded in the log TSV fragment and execution
continues — the batch script always exits 0.
"""
import sys
import csv
import argparse
import subprocess
import logging
from datetime import datetime
from pathlib import Path

from utils import load_ids, load_failed_ids, LOG_HEADER

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_annotations(tsv_path):
    """Return dict of annotation_id → {annotation_url, assembly_url}."""
    annotations = {}
    with open(tsv_path, newline='') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            if not row or not row[0].strip():
                continue
            # Skip header row if present
            if row[0].strip() == 'annotation_id':
                continue
            annotations[row[0].strip()] = {
                'annotation_url': row[1].strip(),
                'assembly_url':   row[2].strip(),
            }
    return annotations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('annotations_tsv', help='Path to annotations.tsv')
    parser.add_argument('log_tsv',         help='Path to log.tsv')
    parser.add_argument('chunk_index',     type=int, help='Index of this chunk (0-based)')
    parser.add_argument('chunk_count',     type=int, help='Total number of chunks')
    parser.add_argument('output_dir',      help='Directory to write result/log fragments')
    parser.add_argument('max_per_job',     type=int, nargs='?', default=None,
                        help='Cap on annotations processed by this chunk')
    parser.add_argument('--retry-failed',  action='store_true',
                        help='Process previously failed annotations instead of new pending ones')
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    annotations = load_annotations(args.annotations_tsv)
    if args.retry_failed:
        pending_ids = sorted(load_failed_ids(args.log_tsv))
        logger.info(f"Retry mode: targeting {len(pending_ids)} previously failed annotations")
    else:
        logged_ids  = load_ids(args.log_tsv)
        pending_ids = sorted(set(annotations.keys()) - logged_ids)  # sorted for determinism

    my_slice = pending_ids[args.chunk_index::args.chunk_count]
    if args.max_per_job is not None:
        my_slice = my_slice[:args.max_per_job]

    logger.info(f"Chunk {args.chunk_index}/{args.chunk_count}: "
                f"{len(my_slice)} annotations to process"
                + (f" (capped at {args.max_per_job})" if args.max_per_job else ""))

    script = Path(__file__).parent / 'run_busco_analysis.py'
    succeeded = 0
    failed    = 0

    for i, annotation_id in enumerate(my_slice, 1):
        ann = annotations[annotation_id]
        result_tsv   = str(output_dir / f"result_{annotation_id}.tsv")
        log_fragment = str(output_dir / f"log_{annotation_id}.tsv")

        logger.info(f"[{i}/{len(my_slice)}] Processing {annotation_id}")

        try:
            ret = subprocess.run(
                [sys.executable, str(script),
                 ann['annotation_url'],
                 ann['assembly_url'],
                 annotation_id,
                 result_tsv,
                 log_fragment],
                check=False   # do not raise on non-zero exit
            )
            if ret.returncode == 0:
                succeeded += 1
                logger.info(f"  ✓ {annotation_id}")
            else:
                failed += 1
                logger.warning(f"  ✗ {annotation_id} (exit {ret.returncode})")
        except Exception as e:
            failed += 1
            logger.error(f"  ✗ {annotation_id} — unexpected error: {e}")
            # Write a log fragment so the aggregator records this failure
            # and the annotation is not silently rescheduled
            try:
                with open(log_fragment, 'w', newline='') as lf:
                    w = csv.writer(lf, delimiter='\t')
                    w.writerow(LOG_HEADER)
                    w.writerow([annotation_id,
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'fail', 'unexpected_error'])
            except Exception as write_err:
                logger.error(f"  Could not write log fragment: {write_err}")

    logger.info(f"Chunk {args.chunk_index} complete: "
                f"{succeeded} succeeded, {failed} failed out of {len(my_slice)}")
    # Always exit 0 — individual failures are recorded in log fragments
    sys.exit(0)


if __name__ == '__main__':
    main()
