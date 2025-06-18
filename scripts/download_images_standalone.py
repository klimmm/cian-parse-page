#!/usr/bin/env python3
"""
Standalone script for downloading images in GitHub Actions environment.
This script can be run independently without the full scheduler.
"""

import os
import sys
import json
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from image_utils.download_images import download_and_predict_images
from utils.image_filter import prefilter_listings_for_download
from utils.image_dedup import dedupe_images


def main():
    parser = argparse.ArgumentParser(description='Download and process images for listings')
    parser.add_argument('--json-file', required=True, help='JSON file with listings data')
    parser.add_argument('--image-dir', required=True, help='Directory to save images')
    parser.add_argument('--model-path', required=True, help='Path to ML model file')
    parser.add_argument('--batch-size', type=int, default=5, help='Batch size for processing')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum retry attempts')
    parser.add_argument('--workers', type=int, default=2, help='Number of worker threads')
    parser.add_argument('--no-dedup', action='store_true', help='Skip deduplication step')
    parser.add_argument('--filter-only', action='store_true', help='Only show what would be downloaded')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not os.path.exists(args.json_file):
        print(f"❌ JSON file not found: {args.json_file}")
        sys.exit(1)
        
    if not os.path.exists(args.model_path):
        print(f"❌ Model file not found: {args.model_path}")
        sys.exit(1)
    
    # Create image directory
    os.makedirs(args.image_dir, exist_ok=True)
    
    # Load JSON data
    print(f"📖 Loading listings from {args.json_file}")
    try:
        with open(args.json_file, 'r', encoding='utf-8') as f:
            listings = json.load(f)
        print(f"✅ Loaded {len(listings)} listings")
    except Exception as e:
        print(f"❌ Error loading JSON file: {e}")
        sys.exit(1)
    
    # Filter listings that need images
    print(f"🔍 Filtering listings for image download...")
    filtered_listings = prefilter_listings_for_download(listings, args.image_dir)
    
    if not filtered_listings:
        print('✅ No listings need image downloading')
        return
    
    print(f"📊 Found {len(filtered_listings)} listings needing images")
    
    if args.filter_only:
        print("📋 Listings that would be processed:")
        for listing in filtered_listings:
            offer_id = listing.get('offer_id')
            image_count = len(listing.get('image_urls', []))
            print(f"  - {offer_id}: {image_count} images")
        return
    
    # Download and predict images
    print(f"🖼️ Starting image download and prediction...")
    print(f"📋 Settings: batch_size={args.batch_size}, max_retries={args.max_retries}, workers={args.workers}")
    
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
        for listing in filtered_listings:
            offer_id = str(listing.get('offer_id'))
            offer_dir = os.path.join(args.image_dir, offer_id)
            if os.path.exists(offer_dir):
                dedupe_images(offer_dir, hash_size=8, max_distance=0)
        print("✅ Image deduplication completed!")
    
    # Print summary
    total_dirs = sum(1 for _ in Path(args.image_dir).iterdir() if _.is_dir())
    total_images = len(list(Path(args.image_dir).rglob("*.jpg"))) + \
                  len(list(Path(args.image_dir).rglob("*.jpeg"))) + \
                  len(list(Path(args.image_dir).rglob("*.png")))
    
    print(f"\n📊 Summary:")
    print(f"   - Processed listings: {len(filtered_listings)}")
    print(f"   - Total directories: {total_dirs}")
    print(f"   - Total images: {total_images}")


if __name__ == '__main__':
    main()