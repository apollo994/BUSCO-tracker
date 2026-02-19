#!/bin/bash

set -euo pipefail

# Check arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <gff_file> <fasta_file>"
    echo "Example: $0 annotation.gff3.gz genome.fna.gz"
    exit 1
fi

GFF_INPUT=$(realpath "$1")
FASTA_INPUT=$(realpath "$2")

# Check if files exist
if [ ! -f "$GFF_INPUT" ]; then
    echo "Error: GFF file '$GFF_INPUT' not found"
    exit 1
fi

if [ ! -f "$FASTA_INPUT" ]; then
    echo "Error: FASTA file '$FASTA_INPUT' not found"
    exit 1
fi

# Get the directory where input files are located
INPUT_DIR=$(dirname "$GFF_INPUT")

# Get base names
GFF_BASENAME=$(basename "$GFF_INPUT" .gz)
GFF_BASENAME=$(basename "$GFF_BASENAME" .gff3)
GFF_BASENAME=$(basename "$GFF_BASENAME" .gff)

# Decompress files if needed
WORK_DIR=$(mktemp -d)
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Working directory: $WORK_DIR"

if [[ "$GFF_INPUT" == *.gz ]]; then
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] Decompressing GFF file..."
    gunzip -c "$GFF_INPUT" > "$WORK_DIR/input.gff3"
    GFF_FILE="$WORK_DIR/input.gff3"
else
    cp "$GFF_INPUT" "$WORK_DIR/input.gff3"
    GFF_FILE="$WORK_DIR/input.gff3"
fi

if [[ "$FASTA_INPUT" == *.gz ]]; then
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] Decompressing FASTA file..."
    gunzip -c "$FASTA_INPUT" > "$WORK_DIR/input.fna"
    FASTA_FILE="$WORK_DIR/input.fna"
else
    cp "$FASTA_INPUT" "$WORK_DIR/input.fna"
    FASTA_FILE="$WORK_DIR/input.fna"
fi

# Step 1: Extract longest isoforms
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Extracting longest isoforms..."
LONGISOFORM_GFF="$WORK_DIR/${GFF_BASENAME}_longisoforms.gff3"
agat_sp_keep_longest_isoform.pl \
    -gff "$GFF_FILE" \
    -o "$LONGISOFORM_GFF"

# Step 2: Extract protein sequences
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Extracting protein sequences..."
PROTEIN_OUTPUT="$INPUT_DIR/${GFF_BASENAME}_proteins.faa"
agat_sp_extract_sequences.pl \
    -f "$FASTA_FILE" \
    -g "$LONGISOFORM_GFF" \
    -t CDS \
    -p \
    -o "$PROTEIN_OUTPUT"

# Clean up temporary files
echo "[$(date +"%Y-%m-%d %H:%M:%S")] Cleaning up temporary files..."
rm -rf "$WORK_DIR"

echo "[$(date +"%Y-%m-%d %H:%M:%S")] Done! Protein sequences saved to: $PROTEIN_OUTPUT"
