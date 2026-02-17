#!/usr/bin/env python3
"""
BUSCO pipeline to process genome annotations.

Steps:
1. Download annotation and assembly files
2. Fix annotation aliases with annocli
3. Unzip files
4. Extract longest isoform with AGAT
5. Extract protein sequences with AGAT
6. Run BUSCO analysis
7. Parse and return results
"""
import os
import sys
import subprocess
import gzip
import shutil
import tempfile
import logging
from pathlib import Path
import urllib.request
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Custom exception for pipeline step failures."""
    def __init__(self, step, message):
        self.step = step
        self.message = message
        super().__init__(f"Step '{step}' failed: {message}")


def download_file(url, output_path):
    """Download a file from URL to output_path."""
    logger.info(f"Downloading {url}...")
    try:
        urllib.request.urlretrieve(url, output_path)
        logger.info(f"Downloaded to {output_path}")
        return output_path
    except Exception as e:
        raise PipelineError("download", f"Failed to download {url}: {str(e)}")


def unzip_file(gzip_path):
    """Unzip a gzip file and return the unzipped path."""
    logger.info(f"Unzipping {gzip_path}...")
    try:
        output_path = str(gzip_path).replace('.gz', '')
        with gzip.open(gzip_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        logger.info(f"Unzipped to {output_path}")
        return output_path
    except Exception as e:
        raise PipelineError("unzip", f"Failed to unzip {gzip_path}: {str(e)}")


def run_command(cmd, step_name, cwd=None, shell=False):
    """Run a shell command and handle errors."""
    logger.info(f"Running {step_name}: {' '.join(cmd) if not shell else cmd}")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            cwd=cwd,
            shell=shell
        )
        logger.info(f"{step_name} completed successfully")
        return result.stdout
    except subprocess.CalledProcessError as e:
        error_msg = f"Command failed with exit code {e.returncode}\nStdout: {e.stdout}\nStderr: {e.stderr}"
        raise PipelineError(step_name, error_msg)
    except FileNotFoundError:
        raise PipelineError(step_name, f"Command not found: {cmd[0]}")


def fix_annotation_alias(annotation_file, assembly_file, work_dir):
    """Fix annotation aliases using annocli."""
    output_file = work_dir / "aliased_annotation.gff"
    cmd = [
        "annocli", "alias",
        str(annotation_file),
        str(assembly_file),
        "--output", str(output_file)
    ]
    run_command(cmd, "annocli_alias")
    return output_file


def extract_longest_isoform(gff_file, work_dir):
    """Extract longest isoform using AGAT."""
    output_file = work_dir / "longest_isoform.gff"
    cmd = [
        "agat_sp_keep_longest_isoform.pl",
        "-gff", str(gff_file),
        "-o", str(output_file)
    ]
    run_command(cmd, "extract_longest_isoform")
    return output_file


def extract_protein_sequences(fasta_file, gff_file, work_dir):
    """Extract protein sequences using AGAT."""
    output_file = work_dir / "proteins.faa"
    cmd = [
        "agat_sp_extract_sequences.pl",
        "-f", str(fasta_file),
        "-g", str(gff_file),
        "-t", "CDS",
        "-p",
        "-o", str(output_file)
    ]
    run_command(cmd, "extract_protein_sequences")
    return output_file


def run_busco(protein_file, work_dir):
    """Run BUSCO analysis on protein sequences."""
    output_name = "busco_output"
    cmd = [
        "busco",
        "-m", "protein",
        "-i", str(protein_file.name),
        "-l", "eukaryota_odb12",
        "-o", output_name,
        "-c", "2"  # Use 2 CPUs
    ]
    run_command(cmd, "busco_analysis", cwd=str(work_dir))
    return work_dir / output_name


def parse_busco_results(busco_output_dir):
    """Parse BUSCO results from the output directory."""
    logger.info("Parsing BUSCO results...")
    
    # Find the short_summary file
    summary_files = list(busco_output_dir.glob("short_summary.*.txt"))
    if not summary_files:
        raise PipelineError("parse_results", "BUSCO summary file not found")
    
    summary_file = summary_files[0]
    logger.info(f"Reading summary from {summary_file}")
    
    with open(summary_file, 'r') as f:
        content = f.read()
    
    # Parse the summary file
    results = {
        'lineage': '',
        'busco_count': 0,
        'complete': 0,
        'single': 0,
        'duplicated': 0,
        'fragmented': 0,
        'missing': 0
    }
    
    # Extract lineage
    lineage_match = re.search(r'lineage dataset is: (\S+)', content)
    if lineage_match:
        results['lineage'] = lineage_match.group(1)
    
    # Extract metrics
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
    count_match = re.search(r'(\d+)\s+total BUSCO groups searched', content)
    if count_match:
        results['busco_count'] = int(count_match.group(1))
    
    logger.info(f"BUSCO results: {results}")
    return results


def run_pipeline(annotation_url, assembly_url, annotation_id):
    """
    Run the complete BUSCO pipeline.
    
    Returns:
        dict: Results containing lineage, busco_count, complete, single, 
              duplicated, fragmented, missing
        
    Raises:
        PipelineError: If any step fails, with step name and error message
    """
    work_dir = Path(tempfile.mkdtemp(prefix=f"busco_{annotation_id}_"))
    logger.info(f"Working directory: {work_dir}")
    
    try:
        # Step 1: Download files
        annotation_gz = work_dir / "annotation.gff.gz"
        assembly_gz = work_dir / "assembly.fna.gz"
        
        download_file(annotation_url, annotation_gz)
        download_file(assembly_url, assembly_gz)
        
        # Step 2: Unzip files
        annotation_file = Path(unzip_file(annotation_gz))
        assembly_file = Path(unzip_file(assembly_gz))
        
        # Step 3: Fix annotation aliases
        aliased_annotation = fix_annotation_alias(annotation_file, assembly_file, work_dir)
        
        # Step 4: Extract longest isoform
        longest_isoform_gff = extract_longest_isoform(aliased_annotation, work_dir)
        
        # Step 5: Extract protein sequences
        protein_file = extract_protein_sequences(assembly_file, longest_isoform_gff, work_dir)
        
        # Step 6: Run BUSCO
        busco_output = run_busco(protein_file, work_dir)
        
        # Step 7: Parse results
        results = parse_busco_results(busco_output)
        
        logger.info("Pipeline completed successfully")
        return results
        
    except PipelineError:
        raise
    except Exception as e:
        raise PipelineError("unknown", str(e))
    finally:
        # Cleanup: Remove working directory
        if work_dir.exists():
            logger.info(f"Cleaning up {work_dir}")
            shutil.rmtree(work_dir, ignore_errors=True)


def main():
    """Main entry point for command-line execution."""
    if len(sys.argv) != 4:
        print("Usage: busco_pipeline.py <annotation_url> <assembly_url> <annotation_id>")
        sys.exit(1)
    
    annotation_url = sys.argv[1]
    assembly_url = sys.argv[2]
    annotation_id = sys.argv[3]
    
    try:
        results = run_pipeline(annotation_url, assembly_url, annotation_id)
        print(f"Success: {results}")
        sys.exit(0)
    except PipelineError as e:
        print(f"Failed at step: {e.step}")
        print(f"Error: {e.message}")
        sys.exit(1)


if __name__ == "__main__":
    main()
