#!/usr/bin/env python3
"""
Download data dictionary files from s3.

This script reads s3_file_list.csv and downloads files from the Data_dictionary
folder (e.g., data dictionaries, documentation files).

Usage:
    python3 download_non_table_files.py

Requirements:
    - boto3: pip install boto3
    - AWS credentials (configured in script or via AWS CLI)
    - s3_file_list.csv in the same directory

The script will:
    1. Identify files in the Data_dictionary folder
    2. Download them to the data_loader folder, preserving directory structure
"""

import csv
import os
import boto3
from pathlib import Path
from urllib.parse import urlparse
import sys

# AWS credentials - use environment variables for security
# Set these in your environment: export AWS_ACCESS_KEY_ID=... and export AWS_SECRET_ACCESS_KEY=...
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'YOUR_AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'YOUR_AWS_SECRET_ACCESS_KEY')
S3_BUCKET = 'afs-akkio'

# Only download files from the Data_dictionary folder
DATA_DICTIONARY_PREFIX = 'files_from_affinity/Data_dictionary/'


def is_data_dictionary_file(s3_path):
    """Check if a file is in the Data_dictionary folder."""
    # Remove s3://bucket/ prefix
    path = s3_path.replace(f's3://{S3_BUCKET}/', '')
    # Check if this file is in the Data_dictionary folder
    return path.startswith(DATA_DICTIONARY_PREFIX)


def get_files_to_download(csv_file):
    """Read CSV and return list of files in Data_dictionary folder."""
    files_to_download = []
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            s3_path = row['name']
            if is_data_dictionary_file(s3_path):
                files_to_download.append({
                    's3_path': s3_path,
                    'size': row.get('size', '0'),
                    'md5': row.get('md5', ''),
                })
    
    return files_to_download


def download_file(s3_client, s3_path, local_path):
    """Download a single file from S3."""
    try:
        # Extract key from s3://bucket/key format
        key = s3_path.replace(f's3://{S3_BUCKET}/', '')
        
        # Create local directory if it doesn't exist
        local_dir = os.path.dirname(local_path)
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)
        
        # Download file
        s3_client.download_file(S3_BUCKET, key, local_path)
        return True
    except Exception as e:
        print(f"Error downloading {s3_path}: {e}", file=sys.stderr)
        return False


def main():
    """Main function to download non-table files."""
    # Get script directory
    script_dir = Path(__file__).parent
    csv_file = script_dir / 's3_file_list.csv'
    output_dir = script_dir
    
    if not csv_file.exists():
        print(f"Error: {csv_file} not found!", file=sys.stderr)
        sys.exit(1)
    
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    # Get list of files to download
    print("Reading s3_file_list.csv...")
    files_to_download = get_files_to_download(csv_file)
    
    if not files_to_download:
        print("No files to download.")
        return
    
    print(f"\nFound {len(files_to_download)} files to download from Data_dictionary folder")
    print("\nFiles to download:")
    for f in files_to_download[:10]:  # Show first 10
        print(f"  - {f['s3_path']}")
    if len(files_to_download) > 10:
        print(f"  ... and {len(files_to_download) - 10} more files")
    
    # Ask for confirmation
    response = input(f"\nDownload {len(files_to_download)} files? (y/n): ")
    if response.lower() != 'y':
        print("Download cancelled.")
        return
    
    # Download files
    print("\nDownloading files...")
    downloaded = 0
    failed = 0
    
    for file_info in files_to_download:
        s3_path = file_info['s3_path']
        # Convert S3 path to local path
        # Remove s3://bucket/ prefix and use as relative path
        relative_path = s3_path.replace(f's3://{S3_BUCKET}/', '')
        local_path = output_dir / relative_path
        
        print(f"Downloading: {relative_path}")
        if download_file(s3_client, s3_path, str(local_path)):
            downloaded += 1
        else:
            failed += 1
    
    print(f"\nDownload complete!")
    print(f"  Successfully downloaded: {downloaded} files")
    if failed > 0:
        print(f"  Failed: {failed} files", file=sys.stderr)


if __name__ == '__main__':
    main()

