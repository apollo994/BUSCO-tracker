#!/usr/bin/env python3
"""Shared utilities for BUSCO-tracker scripts."""
import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

BUSCO_HEADER = ['annotation_id', 'lineage', 'busco_count', 'complete',
                'single', 'duplicated', 'fragmented', 'missing']
LOG_HEADER = ['annotation_id', 'run_at', 'result', 'step']


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
        has_header = first_line.split('\t')[0].strip() == column
        if has_header:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                if row.get(column):
                    ids.add(row[column])
        else:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                if row and row[0].strip():
                    ids.add(row[0].strip())
    return ids


def load_failed_ids(tsv_path):
    """Return set of annotation_ids whose result == 'fail' in a log TSV."""
    p = Path(tsv_path)
    if not p.exists():
        return set()
    ids = set()
    with open(p, newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            if row.get('result') == 'fail' and row.get('annotation_id'):
                ids.add(row['annotation_id'])
    return ids
