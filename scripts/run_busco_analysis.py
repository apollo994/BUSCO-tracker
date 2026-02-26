#!/usr/bin/env python3
"""
BUSCO Analysis Orchestrator

Downloads annotation and assembly files from URLs, then runs the full
BUSCO pipeline using the shell scripts.

Usage:
    python run_busco_analysis.py <annotation_url> <assembly_url> <annotation_id> <busco_tsv> <log_tsv>

Example:
    python run_busco_analysis.py \\
        https://example.com/annotation.gff.gz \\
        https://example.com/assembly.fna.gz \\
        ann123 BUSCO.tsv log.tsv
"""
import csv
import logging
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def download_file(url, dest_path):
    """
    Download a file from a URL to dest_path.

    Returns:
        tuple: (success: bool, error_message: str)
    """
    logger.info(f"Downloading {url} -> {dest_path}")
    try:
        urllib.request.urlretrieve(url, dest_path)
        logger.info(f"Download complete: {dest_path}")
        return True, "NA"
    except urllib.error.HTTPError as e:
        msg = f"HTTP {e.code} downloading {url}: {e.reason}"
        logger.error(msg)
        return False, msg
    except urllib.error.URLError as e:
        msg = f"URL error downloading {url}: {e.reason}"
        logger.error(msg)
        return False, msg
    except Exception as e:
        msg = f"Unexpected error downloading {url}: {e}"
        logger.error(msg)
        return False, msg


def run_shell_script(script_path, args, step_name):
    """
    Run a shell script and return success/failure.

    Returns:
        tuple: (success: bool, stdout: str, stderr: str)
    """
    cmd = [str(script_path)] + args
    logger.info(f"Running {step_name}: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info(f"{step_name} completed successfully")
        return True, result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        logger.error(f"{step_name} failed with exit code {e.returncode}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        return False, e.stdout, e.stderr
    except FileNotFoundError as e:
        logger.error(f"Script not found: {script_path}")
        return False, "", str(e)


def parse_busco_results(busco_output_dir):
    """
    Parse BUSCO results from the output directory.

    Returns:
        dict with lineage, busco_count, complete, single,
        duplicated, fragmented, missing
    """
    logger.info(f"Parsing BUSCO results from {busco_output_dir}")

    summary_files = list(Path(busco_output_dir).glob("short_summary.*.txt"))
    if not summary_files:
        raise ValueError(f"BUSCO summary file not found in {busco_output_dir}")

    summary_file = summary_files[0]
    logger.info(f"Reading summary from {summary_file}")

    content = summary_file.read_text()

    results: dict[str, str | float | int] = {
        "lineage": "",
        "busco_count": "NA",
        "complete": "NA",
        "single": "NA",
        "duplicated": "NA",
        "fragmented": "NA",
        "missing": "NA",
    }

    lineage_match = re.search(r"lineage dataset is: (\S+)", content)
    complete_match = re.search(r"C:(\d+(?:\.\d+)?)%", content)
    single_match = re.search(r"S:(\d+(?:\.\d+)?)%", content)
    duplicated_match = re.search(r"D:(\d+(?:\.\d+)?)%", content)
    fragmented_match = re.search(r"F:(\d+(?:\.\d+)?)%", content)
    missing_match = re.search(r"M:(\d+(?:\.\d+)?)%", content)
    count_match = re.search(r"(\d+)\s+total BUSCO", content, re.IGNORECASE)

    if lineage_match:
        results["lineage"] = str(lineage_match.group(1))
    if complete_match:
        results["complete"] = float(str(complete_match.group(1)))
    if single_match:
        results["single"] = float(str(single_match.group(1)))
    if duplicated_match:
        results["duplicated"] = float(str(duplicated_match.group(1)))
    if fragmented_match:
        results["fragmented"] = float(str(fragmented_match.group(1)))
    if missing_match:
        results["missing"] = float(str(missing_match.group(1)))
    if count_match:
        results["busco_count"] = int(str(count_match.group(1)))

    logger.info(f"BUSCO results: {results}")
    return results


def append_to_busco_tsv(busco_file, annotation_id, results):
    """Append successful BUSCO results to BUSCO.tsv."""
    logger.info(f"Writing results for {annotation_id} to {busco_file}")
    file_exists = Path(busco_file).exists()
    with open(busco_file, "a", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        if not file_exists:
            writer.writerow(
                [
                    "annotation_id",
                    "lineage",
                    "busco_count",
                    "complete",
                    "single",
                    "duplicated",
                    "fragmented",
                    "missing",
                ]
            )
        writer.writerow(
            [
                annotation_id,
                results["lineage"],
                results["busco_count"],
                results["complete"],
                results["single"],
                results["duplicated"],
                results["fragmented"],
                results["missing"],
            ]
        )


def append_to_log_tsv(log_file, annotation_id, result, step):
    """
    Append execution log to log.tsv.

    Args:
        result: 'success' or 'fail'
        step:   Failed step name, or 'NA' on success
    """
    logger.info(f"Logging {result} for {annotation_id} to {log_file}")
    file_exists = Path(log_file).exists()
    run_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        if not file_exists:
            writer.writerow(["annotation_id", "run_at", "result", "step"])
        writer.writerow([annotation_id, run_at, result, step])


def main():
    """Main entry point."""
    if len(sys.argv) != 6:
        print(
            "Usage: python run_busco_analysis.py "
            "<annotation_url> <assembly_url> <annotation_id> <busco_tsv> <log_tsv>"
        )
        print()
        print("Arguments:")
        print("  annotation_url - URL to GFF3/GFF annotation file (can be .gz)")
        print("  assembly_url   - URL to FASTA assembly file (can be .gz)")
        print("  annotation_id  - Unique identifier for this annotation")
        print("  busco_tsv      - Path to BUSCO.tsv output file")
        print("  log_tsv        - Path to log.tsv file")
        sys.exit(1)

    annotation_url = sys.argv[1]
    assembly_url = sys.argv[2]
    annotation_id = sys.argv[3]
    busco_tsv = sys.argv[4]
    log_tsv = sys.argv[5]

    logger.info(f"Starting BUSCO analysis for {annotation_id}")

    # Locate shell scripts relative to this file
    script_dir = Path(__file__).parent
    extract_script = script_dir / "01_extract_proteins.sh"
    busco_script = script_dir / "02_run_BUSCO.sh"

    for script in (extract_script, busco_script):
        if not script.exists():
            logger.error(f"Required script not found: {script}")
            append_to_log_tsv(log_tsv, annotation_id, "fail", "script_missing")
            sys.exit(1)

    # Work inside a temp directory so downloads don't pollute the repo
    work_dir = Path(tempfile.mkdtemp(prefix=f"busco_{annotation_id}_"))
    logger.info(f"Working directory: {work_dir}")

    try:
        # ------------------------------------------------------------------
        # Step 1: Download files
        # ------------------------------------------------------------------
        logger.info("=" * 80)
        logger.info("STEP 1: Download files")
        logger.info("=" * 80)

        gff_file = work_dir / "annotation.gff.gz"
        fasta_file = work_dir / "assembly.fna.gz"

        ok, err = download_file(annotation_url, gff_file)
        if not ok:
            append_to_log_tsv(log_tsv, annotation_id, "fail", err)
            sys.exit(1)

        ok, err = download_file(assembly_url, fasta_file)
        if not ok:
            append_to_log_tsv(log_tsv, annotation_id, "fail", err)
            sys.exit(1)

        # ------------------------------------------------------------------
        # Step 2: Alias sequence IDs
        # ------------------------------------------------------------------
        logger.info("=" * 80)
        logger.info("STEP 2: Alias sequence IDs (annocli alias)")
        logger.info("=" * 80)

        alias_gff_file = work_dir / "annotation.aliasMatch.gff3.gz"
        cmd = [
            "annocli",
            "alias",
            str(gff_file),
            str(fasta_file),
            "--output",
            str(alias_gff_file),
        ]
        logger.info(f"Running alias_ids: {' '.join(cmd)}")
        try:
            alias_result = subprocess.run(
                cmd, capture_output=True, text=True, check=True
            )
            logger.info("alias_ids completed successfully")
            alias_stderr = alias_result.stderr
        except subprocess.CalledProcessError as e:
            logger.error(f"alias_ids failed with exit code {e.returncode}")
            logger.error(f"stdout: {e.stdout}")
            logger.error(f"stderr: {e.stderr}")
            append_to_log_tsv(
                log_tsv, annotation_id, "fail", e.stderr if e.stderr else "NA"
            )
            sys.exit(1)
        except FileNotFoundError as e:
            logger.error("annocli not found")
            append_to_log_tsv(log_tsv, annotation_id, "fail", str(e))
            sys.exit(1)

        if not alias_gff_file.exists():
            logger.error(f"Expected aliasMatch file not found: {alias_gff_file}")
            append_to_log_tsv(
                log_tsv, annotation_id, "fail", alias_stderr if alias_stderr else "NA"
            )
            sys.exit(1)

        logger.info(f"AliasMatch annotation file: {alias_gff_file}")

        # ------------------------------------------------------------------
        # Step 3: Extract proteins
        # ------------------------------------------------------------------
        logger.info("=" * 80)
        logger.info("STEP 3: Extract proteins")
        logger.info("=" * 80)

        success, _, stderr = run_shell_script(
            extract_script, [str(alias_gff_file), str(fasta_file)], "extract_proteins"
        )

        if not success:
            append_to_log_tsv(
                log_tsv, annotation_id, "fail", stderr if stderr else "NA"
            )
            sys.exit(1)

        # 01_extract_proteins.sh writes <gff_basename>_proteins.faa next to the gff
        protein_file = work_dir / "annotation_proteins.faa"
        if not protein_file.exists():
            logger.error(f"Expected protein file not found: {protein_file}")
            append_to_log_tsv(
                log_tsv, annotation_id, "fail", stderr if stderr else "NA"
            )
            sys.exit(1)

        logger.info(f"Protein file: {protein_file}")

        # ------------------------------------------------------------------
        # Step 4: Run BUSCO
        # ------------------------------------------------------------------
        logger.info("=" * 80)
        logger.info("STEP 4: Run BUSCO")
        logger.info("=" * 80)

        lineage_candidates = [
            Path("assets/busco_downloads/lineages/eukaryota_odb12"),
            Path("busco_downloads/lineages/eukaryota_odb12"),
            Path("eukaryota_odb12"),
        ]
        lineage_path = next((p for p in lineage_candidates if p.exists()), None)
        if lineage_path is None:
            logger.error(
                "Lineage folder not found. Tried: "
                + ", ".join(str(p) for p in lineage_candidates)
            )
            append_to_log_tsv(log_tsv, annotation_id, "fail", "lineage_missing")
            sys.exit(1)

        busco_output = str(work_dir / f"busco_{annotation_id}")

        success, _, stderr = run_shell_script(
            busco_script,
            [str(protein_file), str(lineage_path), busco_output],
            "run_busco",
        )

        if not success:
            append_to_log_tsv(
                log_tsv, annotation_id, "fail", stderr if stderr else "NA"
            )
            sys.exit(1)

        # ------------------------------------------------------------------
        # Step 5: Parse and record results
        # ------------------------------------------------------------------
        logger.info("=" * 80)
        logger.info("STEP 5: Parse BUSCO results")
        logger.info("=" * 80)

        results = parse_busco_results(busco_output)
        append_to_busco_tsv(busco_tsv, annotation_id, results)
        append_to_log_tsv(log_tsv, annotation_id, "success", "NA")

        logger.info("=" * 80)
        logger.info(f"✓ Successfully completed BUSCO analysis for {annotation_id}")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        append_to_log_tsv(log_tsv, annotation_id, "fail", "unexpected_error")
        sys.exit(1)

    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
        logger.info(f"Cleaned up working directory: {work_dir}")


if __name__ == "__main__":
    sys.exit(main())
