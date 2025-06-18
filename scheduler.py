import subprocess
import schedule
import time
from datetime import datetime
import json
import os

from cian import parse_data
from utils.json_to_csv import convert_json_to_csv
from utils.transform import transform_listings_data
from utils.distance import calculate_and_update_distances
from utils.image_filter import prefilter_listings_for_download
from utils.image_dedup import dedupe_images
from utils.github_actions import trigger_github_actions_workflow, wait_for_workflow_completion
from image_utils.download_images import download_and_predict_images


CSV_FILE = "/Users/klim/Desktop/сian/app/cian_data/combined_data.csv"
parse_page_dir = "/Users/klim/Desktop/networks/cian/parse_page"
json_file_path = "/Users/klim/Desktop/networks/cian/parse_page/data/merged_listings.json"
image_dir = "/Users/klim/Desktop/сian/app/images"
model_path = "/Users/klim/Desktop/networks/cian/parse_page/image_utils/best_model.h5"
git_repo_dir = "/Users/klim/Desktop/сian/app"

# GitHub Actions configuration
USE_GITHUB_ACTIONS = os.getenv("USE_GITHUB_ACTIONS", "false").lower() == "true"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO_OWNER = os.getenv("GITHUB_REPO_OWNER", "your-username")
GITHUB_REPO_NAME = os.getenv("GITHUB_REPO_NAME", "your-repo")
GITHUB_WORKFLOW_ID = "download-images.yml"


def trigger_image_download_github_actions(merged_data):
    """Trigger image downloading via GitHub Actions"""
    try:
        print("🚀 Preparing data for GitHub Actions image processing...")
        print(f"📊 Total listings to process: {len(merged_data)}")
        
        # Save ALL listings to JSON file for GitHub Actions (filtering will happen there)
        temp_json_path = os.path.join(parse_page_dir, "data", "all_listings_for_github.json")
        os.makedirs(os.path.dirname(temp_json_path), exist_ok=True)
        
        with open(temp_json_path, "w", encoding="utf-8") as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=2)
        print(f"💾 Saved all listings to {temp_json_path}")
        
        # Prepare workflow inputs
        workflow_inputs = {
            "json_file": "data/all_listings_for_github.json",
            "batch_size": "5",
            "max_retries": "3"
        }
        
        # Trigger GitHub Actions workflow
        success = trigger_github_actions_workflow(
            repo_owner=GITHUB_REPO_OWNER,
            repo_name=GITHUB_REPO_NAME,
            workflow_id=GITHUB_WORKFLOW_ID,
            github_token=GITHUB_TOKEN,
            inputs=workflow_inputs
        )
        
        if success:
            print("✅ GitHub Actions workflow triggered successfully!")
            
            # Optionally wait for completion
            wait_for_completion = os.getenv("WAIT_FOR_GITHUB_ACTIONS", "false").lower() == "true"
            if wait_for_completion:
                print("⏳ Waiting for GitHub Actions workflow to complete...")
                completion_success = wait_for_workflow_completion(
                    repo_owner=GITHUB_REPO_OWNER,
                    repo_name=GITHUB_REPO_NAME,
                    workflow_id=GITHUB_WORKFLOW_ID,
                    github_token=GITHUB_TOKEN,
                    timeout_minutes=30
                )
                return completion_success
            else:
                print("ℹ️ Workflow triggered, not waiting for completion")
                return True
        else:
            print("❌ Failed to trigger GitHub Actions workflow")
            return False
            
    except Exception as e:
        print(f"❌ Error triggering GitHub Actions: {e}")
        return False


def process_images_locally(merged_data):
    """Process images locally (original behavior)"""
    try:
        # Filter listings that need image downloading
        print("🔍 Filtering listings for image download...")
        filtered_listings = prefilter_listings_for_download(merged_data, image_dir)
        
        if not filtered_listings:
            print("✅ No listings need image downloading")
            return True

        # Download images for filtered listings (with prediction if model loaded)
        print("🖼️ Starting image download and prediction process...")
        download_and_predict_images(filtered_listings, image_dir, model_path)
        print("✅ Image download and prediction completed successfully!")

        # Deduplicate images for all processed listings
        print("🧹 Starting image deduplication...")
        for listing in filtered_listings:
            offer_id = str(listing.get("offer_id"))
            offer_dir = os.path.join(image_dir, offer_id)
            if os.path.exists(offer_dir):
                dedupe_images(offer_dir, hash_size=8, max_distance=0)
        print("✅ Image deduplication completed!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during local image processing: {e}")
        return False


def run_cian_scraper():
    """Run the main Cian scraper"""
    print("\n" + "=" * 60)
    print("Running Cian Scraper...")
    print("=" * 60)

    try:
        # Call parse_data function directly
        merged_data = parse_data(
            num_processes=2, max_retry_attempts=10, json_file_path=json_file_path
        )

        if merged_data:
            print("\n✅ Cian scraper completed successfully")

            # Transform listings data to add calculated fields
            transform_listings_data(merged_data)

            # Calculate and update distances for all listings
            calculate_and_update_distances(merged_data, json_file_path=json_file_path)

            # Handle image downloading - either locally or via GitHub Actions
            if USE_GITHUB_ACTIONS and GITHUB_TOKEN:
                print("\n🚀 Using GitHub Actions for image downloading...")
                success = trigger_image_download_github_actions(merged_data)
                if not success:
                    print("❌ GitHub Actions trigger failed, falling back to local processing")
                    success = process_images_locally(merged_data)
            else:
                print("\n🖼️ Processing images locally...")
                success = process_images_locally(merged_data)
                
            if not success:
                print("❌ Image processing failed")
                return False

            # Remove image_urls from data to keep JSON file size manageable
            print("\n🧹 Removing image_urls from data...")
            for listing in merged_data:
                if "image_urls" in listing:
                    del listing["image_urls"]
            print("✅ image_urls removed from all listings")

            # Overwrite JSON file with updated data (without image_urls)
            print("💾 Overwriting merged_listings.json with cleaned data...")
            try:
                with open(json_file_path, "w", encoding="utf-8") as f:
                    json.dump(merged_data, f, ensure_ascii=False, indent=2)
                print("✅ merged_listings.json updated successfully")
            except Exception as e:
                print(f"❌ Error updating merged_listings.json: {e}")

            # Convert JSON to CSV
            print("\n📊 Starting JSON to CSV conversion...")
            try:
                success = convert_json_to_csv(
                    output_file=CSV_FILE, listings=merged_data
                )
                if success:
                    print("✅ JSON to CSV conversion completed successfully!")
                else:
                    print("❌ JSON to CSV conversion failed!")
            except Exception as e:
                print(f"❌ Error during JSON to CSV conversion: {e}")

            # Image prediction is now integrated into the download process above
            print("✅ Images downloaded and processed with ML predictions")

            return True
        else:
            print("\n❌ Cian scraper failed - no data returned")
            return False

    except Exception as e:
        print(f"❌ Cian scraper failed with exception: {e}")
        return False


def git_commit_and_push():
    """Commit and push CSV file and images to git"""
    print("\n" + "=" * 60)
    print("Git Commit and Push")
    print("=" * 60)

    try:
        # Add CSV file
        print("📁 Adding CSV file to git...")
        result = subprocess.run(
            ["git", "add", CSV_FILE],
            capture_output=True,
            text=True,
            check=True,
            cwd=git_repo_dir,
        )
        if result.stdout:
            print(f"[GIT ADD CSV] {result.stdout}")
        if result.stderr:
            print(f"[GIT ADD CSV] {result.stderr}")

        # Add images directory
        print("🖼️ Adding images directory to git...")
        result = subprocess.run(
            ["git", "add", image_dir],
            capture_output=True,
            text=True,
            check=True,
            cwd=git_repo_dir,
        )
        if result.stdout:
            print(f"[GIT ADD IMAGES] {result.stdout}")
        if result.stderr:
            print(f"[GIT ADD IMAGES] {result.stderr}")

        # Check if there are changes to commit
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            capture_output=True,
            text=True,
            cwd=git_repo_dir,
        )

        if result.returncode == 0:
            print("ℹ️ No changes to commit")
            return

        # Show what will be committed
        print("📋 Changes to be committed:")
        status_result = subprocess.run(
            ["git", "status", "--porcelain", "--cached"],
            capture_output=True,
            text=True,
            cwd=git_repo_dir,
        )
        if status_result.stdout:
            for line in status_result.stdout.strip().split("\n"):
                print(f"[GIT STATUS] {line}")

        # Create commit message with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        commit_message = f"Auto-update data and images on {timestamp}"

        print(f"💾 Committing changes with message: '{commit_message}'")
        result = subprocess.run(
            ["git", "commit", "-m", commit_message],
            capture_output=True,
            text=True,
            check=True,
            cwd=git_repo_dir,
        )
        if result.stdout:
            print(f"[GIT COMMIT] {result.stdout}")
        print("✅ Changes committed successfully")

        # Push to remote
        print("🚀 Pushing to remote repository...")
        result = subprocess.run(
            ["git", "push"],
            capture_output=True,
            text=True,
            check=True,
            cwd=git_repo_dir,
        )
        if result.stdout:
            print(f"[GIT PUSH] {result.stdout}")
        if result.stderr:
            print(f"[GIT PUSH] {result.stderr}")
        print("✅ Changes pushed successfully")

    except subprocess.CalledProcessError as e:
        print(f"❌ Git operation failed: {e}")
        if e.stdout:
            print(f"[GIT ERROR STDOUT] {e.stdout}")
        if e.stderr:
            print(f"[GIT ERROR STDERR] {e.stderr}")
    except Exception as e:
        print(f"❌ Unexpected error during git operations: {e}")


def run_full_pipeline():
    """Run the complete pipeline: scraper -> predict images -> git commit/push"""
    print("\n🚀 Starting full pipeline...")

    # Step 1: Run Cian scraper (includes image prediction)
    if not run_cian_scraper():
        print("❌ Cian scraper failed, stopping pipeline")
        return

    # Step 2: Commit and push to git
    git_commit_and_push()

    print("✅ Full pipeline completed!")


def schedule_tasks():
    """Schedule the pipeline to run at regular intervals"""
    # Run every 6 hours
    schedule.every(6).hours.do(run_full_pipeline)

    # Also schedule for specific times (optional)
    schedule.every().day.at("09:00").do(run_full_pipeline)
    schedule.every().day.at("15:00").do(run_full_pipeline)
    schedule.every().day.at("21:00").do(run_full_pipeline)

    print("📅 Scheduler configured:")
    print("  - Every 6 hours")
    print("  - Daily at 09:00, 15:00, 21:00")
    print("  - Use Ctrl+C to stop")
    print("\n⏰ Current time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("⏭️  Next scheduled runs:")
    for job in schedule.jobs:
        print(f"   - {job.next_run.strftime('%Y-%m-%d %H:%M:%S')}")

    # Run immediately on startup
    print("\n🚀 Running pipeline immediately on startup...")
    run_full_pipeline()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cian Data Pipeline Scheduler")
    parser.add_argument(
        "--once", action="store_true", help="Run pipeline once and exit"
    )
    parser.add_argument(
        "--schedule", action="store_true", help="Run scheduler continuously"
    )

    args = parser.parse_args()

    if args.once:
        run_full_pipeline()
    elif args.schedule:
        schedule_tasks()
        try:
            print("\n⏳ Waiting for next scheduled run...")
            last_status_time = datetime.now()

            while True:
                schedule.run_pending()

                # Show status every 10 minutes
                current_time = datetime.now()
                if (
                    current_time - last_status_time
                ).total_seconds() >= 600:  # 10 minutes
                    print(
                        f"\n💤 Still running... Current time: {current_time.strftime('%H:%M:%S')}"
                    )
                    if schedule.jobs:
                        next_run = min(job.next_run for job in schedule.jobs)
                        time_until = next_run - current_time
                        hours, remainder = divmod(int(time_until.total_seconds()), 3600)
                        minutes, _ = divmod(remainder, 60)
                        print(f"⏰ Next run in: {hours}h {minutes}m")
                    last_status_time = current_time

                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            print("\n👋 Scheduler stopped by user")
    else:
        print("Usage:")
        print("  python scheduler.py --once      # Run pipeline once")
        print("  python scheduler.py --schedule  # Run scheduler continuously")
