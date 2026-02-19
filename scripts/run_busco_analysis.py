#!/usr/bin/env python3
"""
BUSCO Analysis Orchestrator

Runs BUSCO analysis using shell scripts for protein extraction and BUSCO execution.

Usage:
    python run_busco_analysis.py <gff_file> <fasta_file> <annotation_id> <busco_tsv> <log_tsv>

Example:
    python run_busco_analysis.py annotation.gff3.gz genome.fna.gz ann123 BUSCO.tsv log.tsv
"""
import os
import sys
import subprocess
import csv
import re
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_shell_script(script_path, args, step_name):
    """
    Run a shell script and return success/failure.
    
    Args:
        script_path: Path to the shell script
        args: List of arguments to pass to the script
        step_name: Name of the step for logging
        
    Returns:
        tuple: (success: bool, stdout: str, stderr: str)
    """
    cmd = [str(script_path)] + args
    logger.info(f"Running {step_name}: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
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
        dict: Results containing lineage, busco_count, complete, single, 
              duplicated, fragmented, missing
    """
    logger.info(f"Parsing BUSCO results from {busco_output_dir}")
    
    # Find the short_summary file
    summary_files = list(Path(busco_output_dir).glob("short_summary.*.txt"))
    if not summary_files:
        raise ValueError(f"BUSCO summary file not found in {busco_output_dir}")
    
    summary_file = summary_files[0]
    logger.info(f"Reading summary from {summary_file}")
    
    with open(summary_file, 'r') as f:
        content = f.read()
    
    # Parse the summary file
    results = {
        'lineage': '',
        'busco_count': 0,
        'complete': 0.0,
        'single': 0.0,
        'duplicated': 0.0,
        'fragmented': 0.0,
        'missing': 0.0
    }
    
    # Extract lineage
    lineage_match = re.search(r'lineage dataset is: (\S+)', content)
    if lineage_match:
        results['lineage'] = lineage_match.group(1)
    
    # Extract metrics (handle both percentage and count formats)
    complete_match = re.search(r'C:(\d+(?:\.\d+)?)%', content)
    single_match = re.search(r'S:(\d+(?:\.\d+)?)%', content)
    duplicated_match = re.search(r'D:(\d+(?:\.\d+)?)%', content)
    fragmented_match = re.search(r'F:(\d+(?:\.\d+)?)%', content)
    missing_match = re.search(r'M:(\d+(?:\.\d+)?)%', content)
    
    if complete_match:
        results['complete'] = float(complete_match.group(1))
    if single_match:
        results['single'] = float(single_match.group(1))
    if duplicated_match:
        results['duplicated'] = float(duplicated_match.group(1))
    if fragmented_match:
        results['fragmented'] = float(fragmented_match.group(1))
    if missing_match:
        results['missing'] = float(missing_match.group(1))
    
    # Extract BUSCO count
    count_match = re.search(r'(\d+)\s+total BUSCO', content, re.IGNORECASE)
    if count_match:
        results['busco_count'] = int(count_match.group(1))
    
    logger.info(f"BUSCO results: {results}")
    return results


def append_to_busco_tsv(busco_file, annotation_id, results):
    """Append successful BUSCO results to BUSCO.tsv."""
    logger.info(f"Writing results for {annotation_id} to {busco_file}")
    
    # Check if file exists and has header
    file_exists = Path(busco_file).exists()
    
    with open(busco_file, 'a', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        
        # Write header if file doesn't exist
        if not file_exists:
            writer.writerow(['annotation_id', 'lineage', 'busco_count', 'complete', 
                           'single', 'duplicated', 'fragmented', 'missing'])
        
        writer.writerow([
            annotation_id,
            results['lineage'],
            results['busco_count'],
            results['complete'],
            results['single'],
            results['duplicated'],
            results['fragmented'],
            results['missing']
        ])


def append_to_log_tsv(log_file, annotation_id, result, step):
    """
    Append execution log to log.tsv.
    
    Args:
        log_file: Path to log.tsv
        annotation_id: Annotation identifier
        result: 'success' or 'fail'
        step: Failed step name or 'NA' if success
    """
    logger.info(f"Logging {result} for {annotation_id} to {log_file}")
    
    # Check if file exists and has header
    file_exists = Path(log_file).exists()
    
    run_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    with open(log_file, 'a', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        
        # Write header if file doesn't exist
        if not file_exists:
            writer.writerow(['annotation_id', 'run_at', 'result', 'step'])
        
        writer.writerow([annotation_id, run_at, result, step])


def main():
    """Main entry point."""
    if len(sys.argv) != 6:
        print("Usage: python run_busco_analysis.py <gff_file> <fasta_file> <annotation_id> <busco_tsv> <log_tsv>")
        print()
        print("Arguments:")
        print("  gff_file      - Path to GFF3/GFF annotation file (can be .gz)")
        print("  fasta_file    - Path to FASTA reference genome file (can be .gz)")
        print("  annotation_id - Unique identifier for this annotation")
        print("  busco_tsv     - Path to BUSCO.tsv output file")
        print("  log_tsv       - Path to log.tsv file")
        print()
        print("Example:")
        print("  python run_busco_analysis.py annotation.gff3.gz genome.fna.gz ann123 BUSCO.tsv log.tsv")
        sys.exit(1)
    
    gff_file = sys.argv[1]
    fasta_file = sys.argv[2]
    annotation_id = sys.argv[3]
    busco_tsv = sys.argv[4]
    log_tsv = sys.argv[5]
    
    logger.info(f"Starting BUSCO analysis for {annotation_id}")
    logger.info(f"GFF file: {gff_file}")
    logger.info(f"FASTA file: {fasta_file}")
    
    # Get script directory
    script_dir = Path(__file__).parent
    extract_script = script_dir / "01_extract_proteins.sh"
    busco_script = script_dir / "02_run_BUSCO.sh"
    
    # Verify scripts exist
    if not extract_script.exists():
        logger.error(f"Extract proteins script not found: {extract_script}")
        append_to_log_tsv(log_tsv, annotation_id, 'fail', 'script_missing')
        sys.exit(1)
    
    if not busco_script.exists():
        logger.error(f"BUSCO script not found: {busco_script}")
        append_to_log_tsv(log_tsv, annotation_id, 'fail', 'script_missing')
        sys.exit(1)
    
    # Verify input files exist
    if not Path(gff_file).exists():
        logger.error(f"GFF file not found: {gff_file}")
        append_to_log_tsv(log_tsv, annotation_id, 'fail', 'input_missing')
        sys.exit(1)
    
    if not Path(fasta_file).exists():
        logger.error(f"FASTA file not found: {fasta_file}")
        append_to_log_tsv(log_tsv, annotation_id, 'fail', 'input_missing')
        sys.exit(1)
    
    try:
        # Step 1: Extract proteins
        logger.info("=" * 80)
        logger.info("STEP 1: Extract proteins")
        logger.info("=" * 80)
        
        success, stdout, stderr = run_shell_script(
            extract_script,
            [gff_file, fasta_file],
            "extract_proteins"
        )
        
        if not success:
            logger.error("Protein extraction failed")
            append_to_log_tsv(log_tsv, annotation_id, 'fail', 'extract_proteins')
            sys.exit(1)
        
        # Determine protein file path (script saves it in same dir as GFF)
        gff_dir = Path(gff_file).parent
        gff_basename = Path(gff_file).stem
        if gff_basename.endswith('.gff3') or gff_basename.endswith('.gff'):
            gff_basename = Path(gff_basename).stem
        if gff_basename.endswith('.gz'):
            gff_basename = Path(gff_basename).stem
        
        protein_file = gff_dir / f"{gff_basename}_proteins.faa"
        
        if not protein_file.exists():
            logger.error(f"Protein file not found: {protein_file}")
            append_to_log_tsv(log_tsv, annotation_id, 'fail', 'extract_proteins')
            sys.exit(1)
        
        logger.info(f"Protein file created: {protein_file}")
        
        # Step 2: Run BUSCO
        logger.info("=" * 80)
        logger.info("STEP 2: Run BUSCO")
        logger.info("=" * 80)
        
        # Determine lineage folder path
        # Assume lineage is in busco_downloads/lineages/eukaryota_odb12
        lineage_path = Path("assets/busco_downloads/lineages/eukaryota_odb12")
        if not lineage_path.exists():
            # Try alternative path
            lineage_path = Path("eukaryota_odb12")
            if not lineage_path.exists():
                logger.error(f"Lineage folder not found. Tried: {lineage_path}")
                append_to_log_tsv(log_tsv, annotation_id, 'fail', 'lineage_missing')
                sys.exit(1)
        
        busco_output = f"busco_{annotation_id}"
        
        success, stdout, stderr = run_shell_script(
            busco_script,
            [str(protein_file), str(lineage_path), busco_output],
            "run_busco"
        )
        
        if not success:
            logger.error("BUSCO execution failed")
            append_to_log_tsv(log_tsv, annotation_id, 'fail', 'run_busco')
            sys.exit(1)
        
        # Step 3: Parse BUSCO results
        logger.info("=" * 80)
        logger.info("STEP 3: Parse BUSCO results")
        logger.info("=" * 80)
        
        results = parse_busco_results(busco_output)
        
        # Step 4: Write results
        append_to_busco_tsv(busco_tsv, annotation_id, results)
        append_to_log_tsv(log_tsv, annotation_id, 'success', 'NA')
        
        logger.info("=" * 80)
        logger.info(f"✓ Successfully completed BUSCO analysis for {annotation_id}")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        append_to_log_tsv(log_tsv, annotation_id, 'fail', 'unexpected_error')
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
