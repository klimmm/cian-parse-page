#!/usr/bin/env python3
"""
GitHub Actions script for downloading images.
This script handles the complete pipeline including filtering, downloading, and deduplication.
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
    parser = argparse.ArgumentParser(description='Download and process images for listings in GitHub Actions')
    parser.add_argument('--json-file', required=True, help='JSON file with listings data')
    parser.add_argument('--image-dir', required=True, help='Directory to save images')
    parser.add_argument('--model-path', required=True, help='Path to ML model file')
    parser.add_argument('--batch-size', type=int, default=5, help='Batch size for processing')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum retry attempts')
    parser.add_argument('--workers', type=int, default=2, help='Number of worker threads')
    parser.add_argument('--no-dedup', action='store_true', help='Skip deduplication step')
    
    args = parser.parse_args()
    
    print("🚀 Starting GitHub Actions image download pipeline")
    print(f"📋 Configuration:")
    print(f"   - JSON file: {args.json_file}")
    print(f"   - Image directory: {args.image_dir}")
    print(f"   - Model path: {args.model_path}")
    print(f"   - Batch size: {args.batch_size}")
    print(f"   - Max retries: {args.max_retries}")
    print(f"   - Workers: {args.workers}")
    print(f"   - Deduplication: {'disabled' if args.no_dedup else 'enabled'}")
    
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
            all_listings = json.load(f)
        print(f"✅ Loaded {len(all_listings)} total listings")
    except Exception as e:
        print(f"❌ Error loading JSON file: {e}")
        sys.exit(1)
    
    # Filter listings that need images (THIS IS WHERE THE FILTERING HAPPENS IN GITHUB ACTIONS)
    print(f"🔍 Filtering listings for image download...")
    print(f"📊 Checking existing images in {args.image_dir}")
    
    try:
        filtered_listings = prefilter_listings_for_download(all_listings, args.image_dir)
        print(f"📊 Filtering results:")
        print(f"   - Total listings: {len(all_listings)}")
        print(f"   - Need download: {len(filtered_listings)}")
        print(f"   - Skip ratio: {((len(all_listings) - len(filtered_listings)) / len(all_listings) * 100):.1f}%")
    except Exception as e:
        print(f"❌ Error during filtering: {e}")
        sys.exit(1)
    
    if not filtered_listings:
        print('✅ No listings need image downloading - all images are up to date!')
        print("📊 Summary: 0 listings processed, 0 images downloaded")
        return
    
    # Save filtered listings for debugging/logging
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
    
    print("\n✅ GitHub Actions image download pipeline completed successfully!")


if __name__ == '__main__':
    main()