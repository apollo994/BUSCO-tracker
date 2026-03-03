#!/usr/bin/env python3
"""
Fetch annotation data from AnnoTrEive API and generate TSV output.
"""
import requests
import sys
import csv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ANNOTATIONS_API = "https://genome.crg.es/annotrieve/api/v0/annotations"
ASSEMBLIES_API = "https://genome.crg.es/annotrieve/api/v0/assemblies"


def fetch_json(url):
    """Fetch JSON data from URL with error handling."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


def fetch_all_assemblies():
    """Fetch all assemblies and build a lookup dictionary."""
    logger.info("Fetching all assemblies from API...")
    
    assemblies_dict = {}
    offset = 0
    limit = 1000
    
    while True:
        url = f"{ASSEMBLIES_API}?offset={offset}&limit={limit}"
        logger.info(f"Fetching assemblies page with offset={offset}, limit={limit}")
        data = fetch_json(url)
        
        if not data:
            logger.error("Failed to fetch assemblies data")
            break
        
        results = data.get('results', [])
        if not results:
            break
        
        # Build dictionary mapping assembly_accession to download_url
        for assembly in results:
            accession = assembly.get('assembly_accession', '')
            download_url = assembly.get('download_url', '')
            if accession:
                assemblies_dict[accession] = download_url
        
        total = data.get('total', 0)
        logger.info(f"Fetched {len(results)} assemblies (total so far: {len(assemblies_dict)}/{total})")
        
        # Check if we've fetched all assemblies
        if len(assemblies_dict) >= total:
            break
        
        offset += limit
    
    logger.info(f"Built assembly lookup dictionary with {len(assemblies_dict)} entries")
    return assemblies_dict


def main():
    """Main function to fetch annotations and generate TSV."""
    # First, fetch all assemblies and build lookup dictionary
    assemblies_dict = fetch_all_assemblies()
    
    if not assemblies_dict:
        logger.error("No assemblies found, cannot proceed")
        sys.exit(1)
    
    # Now fetch all annotations
    logger.info("Fetching annotations from API...")
    
    all_annotations = []
    offset = 0
    limit = 1000  # Use maximum limit for faster fetching
    
    while True:
        url = f"{ANNOTATIONS_API}?offset={offset}&limit={limit}"
        logger.info(f"Fetching annotations page with offset={offset}, limit={limit}")
        data = fetch_json(url)
        
        if not data:
            logger.error("Failed to fetch annotations data")
            sys.exit(1)
        
        results = data.get('results', [])
        if not results:
            break
        
        all_annotations.extend(results)
        total = data.get('total', 0)
        
        logger.info(f"Fetched {len(results)} annotations (total so far: {len(all_annotations)}/{total})")
        
        # Check if we've fetched all annotations
        if len(all_annotations) >= total:
            break
        
        offset += limit
    
    if not all_annotations:
        logger.error("No annotations found in API response")
        sys.exit(1)
    
    logger.info(f"Found total of {len(all_annotations)} annotations")
    
    # Generate TSV
    output_file = "annotations.tsv"
    with open(output_file, 'w', newline='') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t', lineterminator='\n')
        
        # Write header
        writer.writerow(['annotation_id', 'annotation_url', 'assembly_url'])
        
        # Process each annotation
        missing_assemblies = set()
        for idx, annotation in enumerate(all_annotations, 1):
            annotation_id = annotation.get('annotation_id', '')
            
            # Get annotation URL from source_file_info
            annotation_url = ''
            if 'source_file_info' in annotation and 'url_path' in annotation['source_file_info']:
                annotation_url = annotation['source_file_info']['url_path']
            
            # Look up assembly URL from pre-built dictionary
            assembly_accession = annotation.get('assembly_accession', '')
            assembly_url = assemblies_dict.get(assembly_accession, '')
            
            # Track missing assemblies for reporting
            if assembly_accession and not assembly_url:
                missing_assemblies.add(assembly_accession)
            
            # Write row
            writer.writerow([annotation_id, annotation_url, assembly_url])
            
            if idx % 1000 == 0:
                logger.info(f"Processed {idx}/{len(all_annotations)} annotations...")
    
    if missing_assemblies:
        logger.warning(f"Could not find assembly URLs for {len(missing_assemblies)} assemblies")
    
    logger.info(f"Successfully generated {output_file} with {len(all_annotations)} annotations")


if __name__ == "__main__":
    main()
