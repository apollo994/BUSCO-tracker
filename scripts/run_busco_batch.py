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
import subprocess
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_ids(tsv_path):
    """Return set of annotation_ids from a TSV. Returns empty set if missing."""
    p = Path(tsv_path)
    if not p.exists():
        return set()
    ids = set()
    with open(p, newline='') as f:
        first_line = f.readline()
        if not first_line:
            return ids
        f.seek(0)
        has_header = first_line.split('\t')[0].strip() == 'annotation_id'
        if has_header:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                if row.get('annotation_id'):
                    ids.add(row['annotation_id'])
        else:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                if row and row[0].strip():
                    ids.add(row[0].strip())
    return ids


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
    if len(sys.argv) != 6:
        print("Usage: python run_busco_batch.py "
              "<annotations_tsv> <log_tsv> <chunk_index> <chunk_count> <output_dir>")
        sys.exit(1)

    annotations_tsv = sys.argv[1]
    log_tsv         = sys.argv[2]
    chunk_index     = int(sys.argv[3])
    chunk_count     = int(sys.argv[4])
    output_dir      = Path(sys.argv[5])

    output_dir.mkdir(parents=True, exist_ok=True)

    annotations  = load_annotations(annotations_tsv)
    logged_ids   = load_ids(log_tsv)
    pending_ids  = sorted(set(annotations.keys()) - logged_ids)  # sorted for determinism

    my_slice = pending_ids[chunk_index::chunk_count]

    logger.info(f"Chunk {chunk_index}/{chunk_count}: "
                f"{len(my_slice)} annotations to process")

    script = Path(__file__).parent / 'run_busco_analysis.py'
    succeeded = 0
    failed    = 0

    for i, annotation_id in enumerate(my_slice, 1):
        ann = annotations[annotation_id]
        result_tsv = str(output_dir / f"result_{annotation_id}.tsv")
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
                from datetime import datetime
                import csv as _csv
                frag = Path(log_fragment)
                with open(frag, 'w', newline='') as lf:
                    w = _csv.writer(lf, delimiter='\t')
                    w.writerow(['annotation_id', 'run_at', 'result', 'step'])
                    w.writerow([annotation_id,
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'fail', 'unexpected_error'])
            except Exception as write_err:
                logger.error(f"  Could not write log fragment: {write_err}")

    logger.info(f"Chunk {chunk_index} complete: "
                f"{succeeded} succeeded, {failed} failed out of {len(my_slice)}")
    # Always exit 0 — individual failures are recorded in log fragments
    sys.exit(0)


if __name__ == '__main__':
    main()
