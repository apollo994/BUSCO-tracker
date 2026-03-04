#!/usr/bin/env python3
"""
Check status of BUSCO-tracker data files.

Reads annotations.tsv, BUSCO.tsv, .retry.log, .giveup.log and counts
the number of unique annotation IDs in each file. Appends a timestamped 
row to .status.log.

Usage:
    python check_status.py
"""
import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from utils import load_ids

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# File paths
ANNOTATIONS_TSV = Path('annotations.tsv')
BUSCO_TSV = Path('BUSCO/eukaryota_odb12/BUSCO.tsv')
RETRY_LOG = Path('BUSCO/eukaryota_odb12/.retry.log')
GIVEUP_LOG = Path('BUSCO/eukaryota_odb12/.giveup.log')
STATUS_LOG = Path('BUSCO/eukaryota_odb12/.status.log')

STATUS_HEADER = ['timestamp', 'annotations', 'busco', 'retry', 'giveup']


def ensure_header(tsv_path, header):
    """Write header if the file does not exist yet."""
    if not tsv_path.exists():
        with open(tsv_path, 'w', newline='') as f:
            csv.writer(f, delimiter='\t', lineterminator='\n').writerow(header)


def main():
    # Count unique annotation IDs in each file
    annotations_count = len(load_ids(ANNOTATIONS_TSV))
    busco_count = len(load_ids(BUSCO_TSV))
    retry_count = len(load_ids(RETRY_LOG))
    giveup_count = len(load_ids(GIVEUP_LOG))

    logger.info(f"Status: {annotations_count} annotations, "
                f"{busco_count} BUSCO, "
                f"{retry_count} retry, "
                f"{giveup_count} giveup")

    # Generate timestamp
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Ensure .status.log exists with header
    ensure_header(STATUS_LOG, STATUS_HEADER)

    # Append status row
    with open(STATUS_LOG, 'a', newline='') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerow([timestamp, annotations_count, busco_count, retry_count, giveup_count])

    logger.info(f"Status logged to {STATUS_LOG}")


if __name__ == '__main__':
    main()
