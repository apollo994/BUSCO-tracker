# BUSCO-tracker - Copilot Instructions

## Project Overview

BUSCO-tracker runs BUSCO (Benchmarking Universal Single-Copy Orthologs) analysis on eukaryotic genome annotations via GitHub Actions. It fetches annotations from the AnnoTrEive API, runs BUSCO protein-mode analysis in parallel matrix jobs, and stores results in TSV files committed back to the repository.

## Architecture

Three manually-triggered (`workflow_dispatch`) GitHub Actions workflows form a pipeline:

1. **fetch-annotations.yml** → runs `scripts/fetch_annotations.py` to pull annotation+assembly URLs from the AnnoTrEive API into `annotations.tsv` (columns: `annotation_id`, `annotation_url`, `assembly_url`).

2. **busco-matrix.yml** — the main workflow with three jobs:
   - **setup** → `scripts/build_matrix.py` reads `annotations.tsv`, `BUSCO/eukaryota_odb12/BUSCO.tsv`, and `BUSCO/eukaryota_odb12/error_log.tsv` to find pending annotations (never-run first, then failed retries). Emits a JSON matrix of chunk indices.
   - **busco** → parallel matrix jobs (up to 256). Each runs `scripts/run_busco_batch.py` which strides over the pending list (`pending[chunk_index::chunk_count]`) and calls `scripts/run_busco_analysis.py` per annotation. Each annotation produces a `result_<id>.tsv` or `log_<id>.tsv` fragment.
   - **aggregate** → `scripts/aggregate_results.py` merges fragments into `BUSCO/eukaryota_odb12/BUSCO.tsv` and `BUSCO/eukaryota_odb12/error_log.tsv`, deduplicating by annotation_id. Commits results.

3. **aggregate-results.yml** — standalone re-aggregation from a previous run's artifacts (takes a `run_id` input).

### Per-annotation pipeline (`run_busco_analysis.py`)

Each annotation goes through: download GFF+FASTA → `annocli alias` (sequence ID aliasing) → `01_extract_proteins.sh` (gffread + longest-isoform AWK filter) → `02_run_BUSCO.sh` (BUSCO protein mode, offline, 4 CPUs) → parse `short_summary.*.txt` → write result/error fragment TSV.

### Key data files

- `annotations.tsv` — input registry of all annotations (auto-generated from API)
- `BUSCO/eukaryota_odb12/BUSCO.tsv` — successful results (columns: `annotation_id`, `lineage`, `busco_count`, `complete`, `single`, `duplicated`, `fragmented`, `missing`)
- `BUSCO/eukaryota_odb12/error_log.tsv` — failure log (columns: `annotation_id`, `run_at`, `step`)

## Key Conventions

- **Shared constants** are in `scripts/utils.py` (`BUSCO_HEADER`, `ERROR_LOG_HEADER`, `load_ids()`, `compute_pending_ids()`). Always import from there rather than redefining.
- **Batch jobs always exit 0** — per-annotation failures are recorded in error log fragments, not as job failures. This prevents one bad annotation from cancelling parallel jobs (`fail-fast: false`).
- **Idempotent aggregation** — `aggregate_results.py` deduplicates by `annotation_id` (BUSCO) or `(annotation_id, run_at)` (errors) so re-running is safe.
- **Conda environment** in CI: BUSCO 6.0.0, gffread 0.12.7, and annocli 1.0.0 are installed via mamba with strict channel priority (conda-forge, bioconda). The env is cached by version key.
- **Lineage dataset**: `eukaryota_odb12` is used exclusively, running in offline mode. The lineage path resolution tries multiple locations (see `run_busco_analysis.py`).
- Shell scripts use `set -euo pipefail`. Python scripts use `logging` with INFO level and `pathlib.Path`.
- Python scripts are run with `python scripts/<name>.py` (no package/module install). Only external dependency is `requests` (for `fetch_annotations.py`).
