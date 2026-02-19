#!/bin/bash

# Check if required arguments are provided
if [ $# -lt 3 ]; then
    echo "Error: Missing required arguments"
    echo "Usage: $0 <proteins_file> <lineage_folder> <output_folder> [--cpu N]"
    exit 1
fi

proteins=$1
lineage=$2
outfolder=$3

# Check if proteins exists and is not empty
if [ ! -f "${proteins}" ]; then
    echo "Error: Protein file ${proteins} does not exist"
    exit 1
fi

if [ ! -s "${proteins}" ]; then
    echo "Error: Protein file ${proteins} is empty"
    exit 1
fi

# Check if lineage folder exists
if [ ! -d "${lineage}" ]; then
    echo "Error: Lineage folder ${lineage} does not exist"
    exit 1
fi

# Check if outfolder does not already exist
if [ -d "${outfolder}" ]; then
    echo "Error: Output folder ${outfolder} already exists"
    exit 1
fi

# Check if conda is available
if ! command -v conda &> /dev/null; then
    echo "Error: conda command not found"
    exit 1
fi


OUT_NAME=$(basename "${outfolder}")
OUT_PATH=$(dirname "${outfolder}")

busco -m protein \
	-i ${proteins} \
	-l ${lineage} \
	--cpu 2 \
	--out_path "${OUT_PATH}" \
	-o "${OUT_NAME}"
