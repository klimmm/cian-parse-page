#!/usr/bin/env python3
"""
Smart GitHub Actions script for downloading images.
This script uses sparse checkout to only fetch directories we need.
"""

import os
import sys
import json
import argparse
import subprocess
import requests
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from tqdm import tqdm

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.image_dedup import dedupe_images


def get_existing_directories_from_git(repo_path: str) -> set:
    """Get list of existing image directories from git ls-tree without full checkout."""
    try:
        # Use git ls-tree to list directories in images/ without full checkout
        result = subprocess.run(
            ['git', 'ls-tree', '-d', '--name-only', 'HEAD', 'images/'],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Extract directory names (offer_ids)
        directories = set()
        for line in result.stdout.strip().split('\n'):
            if line.startswith('images/'):
                dir_name = line.replace('images/', '')
                if dir_name:  # Skip empty lines
                    directories.add(dir_name)
        
        print(f"📁 Found {len(directories)} existing directories in cian-tracker")
        return directories
        
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Error getting directories from git: {e}")
        return set()


def count_existing_images_from_git(repo_path: str, offer_id: str) -> int:
    """Count existing images for an offer using git ls-tree."""
    try:
        result = subprocess.run(
            ['git', 'ls-tree', '--name-only', 'HEAD', f'images/{offer_id}/'],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            return 0  # Directory doesn't exist
            
        # Count image files
        image_count = 0
        for line in result.stdout.strip().split('\n'):
            if line and line.lower().endswith(('.jpg', '.jpeg', '.png')):
                image_count += 1
                
        return image_count
        
    except Exception as e:
        print(f"⚠️ Error counting images for {offer_id}: {e}")
        return 0


def smart_prefilter_listings(all_listings: list, repo_path: str) -> list:
    """Filter listings - FORCE DOWNLOAD ALL for now."""
    print(f"🔥 HARDCORE MODE: Downloading ALL {len(all_listings)} listings...")
    
    to_download = []
    
    for listing in all_listings:
        offer_id = str(listing.get("offer_id"))
        urls = listing.get("image_urls") or []
        expected = len(urls)
        
        if expected == 0:
            continue

        print(f"🔥 {offer_id}: FORCE DOWNLOAD {expected} images")
        to_download.append(listing)

    print(f"🆕 {len(to_download)} offers will be downloaded")
    return to_download


def download_image(url: str, filepath: str, max_retries: int = 3) -> bool:
    """Download a single image with retries."""
    print(f"🔥 DEBUG: Attempting to download {url} -> {filepath}")
    
    for attempt in range(max_retries):
        try:
            print(f"🔥 DEBUG: Attempt {attempt + 1} for {url}")
            response = requests.get(url, timeout=30, stream=True)
            print(f"🔥 DEBUG: Got response {response.status_code} for {url}")
            
            if response.status_code == 200:
                print(f"🔥 DEBUG: Creating directory {os.path.dirname(filepath)}")
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                
                print(f"🔥 DEBUG: Writing file {filepath}")
                with open(filepath, 'wb') as f:
                    bytes_written = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        bytes_written += len(chunk)
                
                print(f"🔥 DEBUG: Successfully wrote {bytes_written} bytes to {filepath}")
                
                # Verify file exists and has content
                if os.path.exists(filepath):
                    size = os.path.getsize(filepath)
                    print(f"🔥 DEBUG: File {filepath} exists with size {size} bytes")
                    return True
                else:
                    print(f"🔥 DEBUG: ERROR - File {filepath} does not exist after writing!")
                    return False
            else:
                print(f"⚠️ HTTP {response.status_code} for {url}")
        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
    
    print(f"🔥 DEBUG: All attempts failed for {url}")
    return False


def download_images_for_listings(listings: list, image_dir: str, max_retries: int = 3, workers: int = 2):
    """Download images for all listings."""
    print(f"🖼️ Starting image download for {len(listings)} listings...")
    
    total_downloaded = 0
    total_failed = 0
    
    for listing in tqdm(listings, desc="Processing listings"):
        offer_id = str(listing.get("offer_id"))
        urls = listing.get("image_urls") or []
        
        if not urls:
            continue
            
        offer_dir = os.path.join(image_dir, offer_id)
        os.makedirs(offer_dir, exist_ok=True)
        
        # Download images for this listing
        download_tasks = []
        for i, url in enumerate(urls):
            if not url:
                continue
                
            # Generate filename from URL
            parsed_url = urlparse(url)
            filename = f"image_{i:03d}.jpg"
            if parsed_url.path:
                ext = os.path.splitext(parsed_url.path)[1].lower()
                if ext in ['.jpg', '.jpeg', '.png']:
                    filename = f"image_{i:03d}{ext}"
            
            filepath = os.path.join(offer_dir, filename)
            
            # Skip if already exists
            if os.path.exists(filepath):
                continue
                
            download_tasks.append((url, filepath))
        
        # Download images in parallel for this listing
        if download_tasks:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_url = {
                    executor.submit(download_image, url, filepath, max_retries): url 
                    for url, filepath in download_tasks
                }
                
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        if future.result():
                            total_downloaded += 1
                        else:
                            total_failed += 1
                    except Exception as e:
                        print(f"⚠️ Error downloading {url}: {e}")
                        total_failed += 1
    
    print(f"✅ Download completed: {total_downloaded} downloaded, {total_failed} failed")
    return total_downloaded, total_failed


def setup_sparse_checkout(repo_path: str, filtered_listings: list):
    """Setup sparse checkout for only the directories we need."""
    print(f"🎯 Setting up sparse checkout for {len(filtered_listings)} listings...")
    
    # Get existing directories that we might need
    existing_dirs = get_existing_directories_from_git(repo_path)
    
    # Create sparse-checkout patterns
    patterns = ["images/"]  # Always include the images directory structure
    
    # Only add patterns for directories that already exist
    for listing in filtered_listings:
        offer_id = str(listing.get("offer_id"))
        if offer_id in existing_dirs:
            patterns.append(f"images/{offer_id}/")
    
    # Write sparse-checkout file
    sparse_file = os.path.join(repo_path, '.git', 'info', 'sparse-checkout')
    with open(sparse_file, 'w') as f:
        f.write('\n'.join(patterns))
    
    print(f"📝 Created sparse-checkout with {len(patterns)} patterns (for existing dirs only)")
    
    # Apply sparse checkout
    try:
        subprocess.run(
            ['git', 'read-tree', '-m', '-u', 'HEAD'],
            cwd=repo_path,
            check=True
        )
        print("✅ Sparse checkout applied successfully")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Error applying sparse checkout: {e}")


def main():
    parser = argparse.ArgumentParser(description='Smart image download with sparse checkout')
    parser.add_argument('--json-file', required=True, help='JSON file with listings data')
    parser.add_argument('--image-dir', required=True, help='Directory to save images')
    parser.add_argument('--model-path', required=False, help='Path to ML model file (optional)')
    parser.add_argument('--batch-size', type=int, default=5, help='Batch size for processing')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum retry attempts')
    parser.add_argument('--workers', type=int, default=2, help='Number of worker threads')
    parser.add_argument('--no-dedup', action='store_true', help='Skip deduplication step')
    parser.add_argument('--no-predict', action='store_true', help='Skip ML prediction step')
    
    args = parser.parse_args()
    
    print("🚀 Starting smart GitHub Actions image download pipeline")
    print(f"📋 Configuration:")
    print(f"   - JSON file: {args.json_file}")
    print(f"   - Image directory: {args.image_dir}")
    if not args.no_predict:
        print(f"   - Model path: {args.model_path}")
    print(f"   - Batch size: {args.batch_size}")
    print(f"   - Max retries: {args.max_retries}")
    print(f"   - Workers: {args.workers}")
    print(f"   - Skip predictions: {args.no_predict}")
    
    # Validate inputs
    if not os.path.exists(args.json_file):
        print(f"❌ JSON file not found: {args.json_file}")
        sys.exit(1)
        
    if not args.no_predict and args.model_path and not os.path.exists(args.model_path):
        print(f"❌ Model file not found: {args.model_path}")
        sys.exit(1)
    
    # Get repository path
    repo_path = os.path.dirname(args.image_dir)  # cian-tracker directory
    
    # Load JSON data
    print(f"📖 Loading listings from {args.json_file}")
    try:
        with open(args.json_file, 'r', encoding='utf-8') as f:
            all_listings = json.load(f)
        print(f"✅ Loaded {len(all_listings)} total listings")
    except Exception as e:
        print(f"❌ Error loading JSON file: {e}")
        sys.exit(1)
    
    # Smart filtering (using git ls-tree)
    try:
        filtered_listings = smart_prefilter_listings(all_listings, repo_path)
        print(f"📊 Filtering results:")
        print(f"   - Total listings: {len(all_listings)}")
        print(f"   - Need download: {len(filtered_listings)}")
        if len(all_listings) > 0:
            print(f"   - Skip ratio: {((len(all_listings) - len(filtered_listings)) / len(all_listings) * 100):.1f}%")
    except Exception as e:
        print(f"❌ Error during smart filtering: {e}")
        sys.exit(1)
    
    if not filtered_listings:
        print('✅ No listings need image downloading - all images are up to date!')
        print("📊 Summary: 0 listings processed, 0 images downloaded")
        return
    
    # Skip sparse checkout setup - we need full repo access for committing
    # setup_sparse_checkout(repo_path, filtered_listings)
    
    # Create image directory structure
    os.makedirs(args.image_dir, exist_ok=True)
    
    # Save filtered listings for debugging
    filtered_json_path = os.path.join(os.path.dirname(args.json_file), "filtered_listings_debug.json")
    try:
        with open(filtered_json_path, 'w', encoding='utf-8') as f:
            json.dump(filtered_listings, f, ensure_ascii=False, indent=2)
        print(f"💾 Saved filtered listings to {filtered_json_path} for debugging")
    except Exception as e:
        print(f"⚠️ Warning: Could not save filtered listings: {e}")
    
    # Download images
    if args.no_predict:
        print(f"🖼️ Starting image download for {len(filtered_listings)} listings...")
        try:
            total_downloaded, total_failed = download_images_for_listings(
                listings=filtered_listings,
                image_dir=args.image_dir,
                max_retries=args.max_retries,
                workers=args.workers
            )
            print("✅ Image download completed successfully!")
        except Exception as e:
            print(f"❌ Error during image download: {e}")
            sys.exit(1)
    else:
        # Original prediction mode (would need import)
        print("❌ Prediction mode not supported in this version. Use --no-predict flag.")
        sys.exit(1)
    
    # Deduplicate images
    if not args.no_dedup:
        print("🧹 Starting image deduplication...")
        dedup_count = 0
        for listing in filtered_listings:
            offer_id = str(listing.get('offer_id'))
            offer_dir = os.path.join(args.image_dir, offer_id)
            if os.path.exists(offer_dir):
                before_count = len([f for f in os.listdir(offer_dir) 
                                 if f.lower().endswith(('.jpg', '.jpeg', '.png'))])
                dedupe_images(offer_dir, hash_size=8, max_distance=0)
                after_count = len([f for f in os.listdir(offer_dir) 
                                if f.lower().endswith(('.jpg', '.jpeg', '.png'))])
                dedup_count += (before_count - after_count)
        print(f"✅ Image deduplication completed! Removed {dedup_count} duplicate images")
    else:
        print("⏭️ Skipping deduplication as requested")
    
    # Print final summary
    total_dirs = sum(1 for _ in Path(args.image_dir).iterdir() if _.is_dir())
    total_images = len(list(Path(args.image_dir).rglob("*.jpg"))) + \
                  len(list(Path(args.image_dir).rglob("*.jpeg"))) + \
                  len(list(Path(args.image_dir).rglob("*.png")))
    
    print(f"\n📊 Final Summary:")
    print(f"   - Processed listings: {len(filtered_listings)}")
    print(f"   - Total directories: {total_dirs}")
    print(f"   - Total images: {total_images}")
    if not args.no_dedup:
        print(f"   - Duplicates removed: {dedup_count}")
    
    print("\n✅ Smart GitHub Actions image download pipeline completed successfully!")


if __name__ == '__main__':
    main()