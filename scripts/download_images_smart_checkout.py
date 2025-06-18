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
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from image_utils.download_images import download_and_predict_images
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
    """Filter listings using git ls-tree (without full checkout)."""
    print(f"🔍 Smart filtering {len(all_listings)} listings...")
    
    # Get existing directories from git
    existing_dirs = get_existing_directories_from_git(repo_path)
    
    to_download = []
    
    for listing in all_listings:
        offer_id = str(listing.get("offer_id"))
        urls = listing.get("image_urls") or []
        expected = len(urls)
        
        if expected == 0:
            continue

        # Check if directory exists and count images
        if offer_id in existing_dirs:
            existing = count_existing_images_from_git(repo_path, offer_id)
        else:
            existing = 0
            
        missing = expected - existing

        if existing < expected / 2:  # Only download if less than half of images exist
            print(f"⚠️ {offer_id}: have {existing}/{expected}, missing {missing} images, queuing")
            to_download.append(listing)
        else:
            print(f"⏩ {offer_id}: have {existing}/{expected}, skipping (less than half missing)")

    print(f"🆕 {len(to_download)} offers need images")
    return to_download


def setup_sparse_checkout(repo_path: str, filtered_listings: list):
    """Setup sparse checkout for only the directories we need."""
    print(f"🎯 Setting up sparse checkout for {len(filtered_listings)} listings...")
    
    # Create sparse-checkout patterns
    patterns = ["images/"]  # Always include the images directory structure
    
    for listing in filtered_listings:
        offer_id = str(listing.get("offer_id"))
        patterns.append(f"images/{offer_id}/")
    
    # Write sparse-checkout file
    sparse_file = os.path.join(repo_path, '.git', 'info', 'sparse-checkout')
    with open(sparse_file, 'w') as f:
        f.write('\n'.join(patterns))
    
    print(f"📝 Created sparse-checkout with {len(patterns)} patterns")
    
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
    parser.add_argument('--model-path', required=True, help='Path to ML model file')
    parser.add_argument('--batch-size', type=int, default=5, help='Batch size for processing')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum retry attempts')
    parser.add_argument('--workers', type=int, default=2, help='Number of worker threads')
    parser.add_argument('--no-dedup', action='store_true', help='Skip deduplication step')
    
    args = parser.parse_args()
    
    print("🚀 Starting smart GitHub Actions image download pipeline")
    print(f"📋 Configuration:")
    print(f"   - JSON file: {args.json_file}")
    print(f"   - Image directory: {args.image_dir}")
    print(f"   - Model path: {args.model_path}")
    print(f"   - Batch size: {args.batch_size}")
    print(f"   - Max retries: {args.max_retries}")
    print(f"   - Workers: {args.workers}")
    
    # Validate inputs
    if not os.path.exists(args.json_file):
        print(f"❌ JSON file not found: {args.json_file}")
        sys.exit(1)
        
    if not os.path.exists(args.model_path):
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
    
    # Setup sparse checkout for only needed directories
    setup_sparse_checkout(repo_path, filtered_listings)
    
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
    
    # Download and predict images
    print(f"🖼️ Starting image download and prediction for {len(filtered_listings)} listings...")
    
    try:
        download_and_predict_images(
            parsed_apts=filtered_listings,
            image_dir=args.image_dir,
            model_path=args.model_path,
            max_retries=args.max_retries,
            batch_size=args.batch_size,
            workers=args.workers,
            parallel_mode=True
        )
        print("✅ Image download and prediction completed successfully!")
    except Exception as e:
        print(f"❌ Error during image download: {e}")
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