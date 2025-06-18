import json
import logging
from pathlib import Path
import os
import time
import gc
from urls.urls_list import urls
from vpn_manager.vpn_manager import VPNManager
from scraper.scraper_core import ScraperConfig
from scraper.factory import create_scraper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def save_json(filename, data):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def run_scrapper(
    num_processes=2,
    max_retry_attempts=5,
    urls_to_scrape=urls,
    script_filename="scripts/parse_listing_page.js",
    data_filename="data/test.json",
    use_vpn=True,
    vpn_manager=None,
):
    """Single scraper call

    Args:
        vpn_manager: Optional VPNManager instance. If provided, use_vpn is ignored.
                    If None and use_vpn=True, creates new VPNManager instance.
    """

    temp_file_pattern = None
    if data_filename:
        base_filename = Path(data_filename).stem
        temp_file_pattern = f"{base_filename}_temp_pass_"

    remaining_urls = urls_to_scrape
    successful = []
    failed = []

    # Load script and create scraper instance
    script_path = Path(script_filename)
    with open(script_path, "r", encoding="utf-8") as f:
        parsing_script = f.read()

    config = ScraperConfig(num_processes=num_processes)

    # Handle VPN manager - use provided instance or create new one
    if vpn_manager is not None:
        vpn_mgr = vpn_manager
        cleanup_vpn = False  # Don't cleanup externally provided VPN manager
    elif use_vpn:
        vpn_mgr = VPNManager()
        cleanup_vpn = True  # Cleanup self-created VPN manager
    else:
        vpn_mgr = None
        cleanup_vpn = False

    # Example 1: Using thread-based scraper
    scraper = create_scraper(
        mode="thread",
        cpu_monitoring=True,
        parsing_script=parsing_script,
        config=config,
        vpn_manager=vpn_mgr
    )

    for i in range(max_retry_attempts):
        if not remaining_urls:
            logger.info("No more URLs to process. Stopping.")
            break

        logger.info(f"\nStarting attempt {i+1}")
        logger.info(f"Links to process: {len(remaining_urls)}")

        results = scraper.scrape_all(remaining_urls)

        failed = []
        for r in results:
            if i == 0:
                is_published = r.get("metadata", {}).get("is_unpublished") is False
                missing_estimation = "estimation_price" not in r or not r.get(
                    "estimation_price"
                )
                if is_published and missing_estimation:
                    r["error"] = "Missing estimation_price for published offer"
                    logger.info("error: is_published and missing_estimation")
            if "error" in r:
                if "404" in str(r.get("error", "")):
                    successful.append(r)  # Add 404 errors to successful for processing
                else:
                    failed.append(r)  # Other errors go to failed for retry
            else:
                successful.append(r)

        remaining_urls = [
            r["url"] for r in failed if "404" not in str(r.get("error", ""))
        ]

        if os.path.exists(f"{temp_file_pattern}{i}.json"):
            os.remove(f"data/{temp_file_pattern}{i}.json")

        if i < max_retry_attempts - 1 and remaining_urls:
            logger.info(f"Will retry {len(remaining_urls)} failed URLs")
            if data_filename:
                save_json(f"data/{temp_file_pattern}{i+1}.json", successful + failed)
            delay = 2 * (i + 1)  # Progressive: 2s, 4s, 6s, 8s...
            logger.info(f"Waiting {delay} seconds before next attempt...")
            time.sleep(delay)

    result = successful + failed
    logger.info(
        f"\n✅ results: {len(result)}, "
        f"(sucss: {len(successful)}, failed: {len(failed)})"
    )

    if data_filename is not None:
        save_json(data_filename, result)
        logger.info(f"Saved results to {data_filename}")

    # Explicit cleanup
    logger.info("Cleaning up resources...")
    del scraper
    if cleanup_vpn and vpn_mgr:
        del vpn_mgr
    gc.collect()
    time.sleep(1)  # Brief pause for system cleanup
    logger.info("Resource cleanup completed")

    return successful + failed


if __name__ == "__main__":
    run_scrapper()
