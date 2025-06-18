from scraper.run import run_scrapper
from utils.helpers import (
    construct_search_url,
    load_json_file,
    load_yaml_file,
    save_json_file,
    generate_search_page_urls,
    generate_listing_page_urls,
    flatten_search_results,
    dedupe_listings,
)
from utils.normalize_data import normalize_listings
from utils.merge_data import merge_data
from utils.validation import validate_merge
from vpn_manager.vpn_manager import VPNManager
import copy
import gc


def get_scrapper_calls(num_processes, max_retry_attempts, shared_vpn):
    """Generate scrapper call configurations with shared parameters"""
    return {
        "summary_extraction": {
            "num_processes": 1,
            "max_retry_attempts": 3,
            "urls_to_scrape": None,
            "script_filename": "scripts/extract_summary.js",
            "data_filename": "data/extract_summary.json",
            "use_vpn": False,
            "vpn_manager": None,
        },
        "search_pages": {
            "num_processes": num_processes,
            "max_retry_attempts": max_retry_attempts,
            "urls_to_scrape": None,
            "script_filename": "scripts/parse_search_page.js",
            "data_filename": "data/parse_search_page_data.json",
            "use_vpn": True,
            "vpn_manager": shared_vpn,
        },
        "listing_pages": {
            "num_processes": num_processes,
            "max_retry_attempts": max_retry_attempts,
            "urls_to_scrape": None,
            "script_filename": "scripts/parse_listing_page.js",
            "data_filename": "data/parse_listing_page_data.json",
            "use_vpn": True,
            "vpn_manager": shared_vpn,
        },
    }


def merge_and_validate(existing_listings, new_offer_ids, merge_label):
    normalize_listings(new_offer_ids)
    original_data = copy.deepcopy(existing_listings)
    merged_data = merge_data(existing_listings, new_offer_ids)
    validate_merge(original_data, new_offer_ids, merged_data, merge_label)

    return merged_data


def parse_data(json_file_path, num_processes=2, max_retry_attempts=10):

    shared_vpn = VPNManager()
    print("Created shared VPN manager for all scraping phases")
    scrapper_calls = get_scrapper_calls(num_processes, max_retry_attempts, shared_vpn)
    search_config = load_yaml_file("search_config.yaml")
    base_url = construct_search_url(search_config)
    print(f"base_url {base_url}")

    print("\nExecuting scraping operations...")

    # Phase 1: Extract summary to determine total pages
    print("\nExtract total listings count from base URL")
    scrapper_calls["summary_extraction"]["urls_to_scrape"] = [base_url]
    search_summary = run_scrapper(**scrapper_calls["summary_extraction"])
    search_page_urls = generate_search_page_urls(base_url, search_summary)

    # Phase 2: Scrape search pages
    print("Scrape all search result pages")
    scrapper_calls["search_pages"]["urls_to_scrape"] = search_page_urls
    search_results = run_scrapper(**scrapper_calls["search_pages"])
    flattened_search_results = flatten_search_results(search_results)
    listings_in_search = dedupe_listings(flattened_search_results)

    # Phase 3: Normizlize and merge search results
    existing_listings = load_json_file(json_file_path)
    normalize_listings(listings_in_search)
    merged_data = merge_and_validate(
        existing_listings, listings_in_search, "SEARCH MERGE"
    )

    # Phase 4: Identify listing pages to scrape
    existing_active_offer_ids = {
        listing["offer_id"]
        for listing in existing_listings
        if not listing.get("metadata", {}).get("is_unpublished", False)
    }
    offer_ids_in_search = {listing["offer_id"] for listing in listings_in_search}
    new_offer_ids = offer_ids_in_search - existing_active_offer_ids
    missing_offer_ids = existing_active_offer_ids - offer_ids_in_search
    listings_to_scrape = list(new_offer_ids | missing_offer_ids)

    print(f"Found {len(existing_active_offer_ids)} existing_active_offer_ids")
    print(f"Found {len(offer_ids_in_search)} offer_ids_in_search")
    print(f"Found {len(new_offer_ids)} new_offer_ids")
    print(f"Found {len(missing_offer_ids)} missing_offer_ids")

    # Phase 5: Scrape listing pages if needed
    if listings_to_scrape:

        print(f"Found {len(listings_to_scrape)} listings_to_scrape")

        print("Scrape individual listing detail pages")
        listing_page_urls = generate_listing_page_urls(listings_to_scrape)
        scrapper_calls["listing_pages"]["urls_to_scrape"] = listing_page_urls
        parsed_listings = run_scrapper(**scrapper_calls["listing_pages"])

        if parsed_listings:
            # Handle 404 errors by marking listings as unpublished
            for listing in parsed_listings:
                if listing.get("error") and "404" in str(listing.get("error")):
                    listing["metadata"] = {"is_unpublished": True}
                    offer_id = None
                    if listing.get("url"):
                        import re

                        match = re.search(r"/(\d+)/?$", listing["url"])
                        if match:
                            offer_id = match.group(1)
                            listing["offer_id"] = offer_id
                    print(
                        f"Marked listing {offer_id or 'unknown'} as unpublished due to 404 error"
                    )

            # Remove listings that are missing from search but still active
            missing_active_listings = []
            for listing in parsed_listings:
                offer_id = listing.get("offer_id")
                is_unpublished = listing.get("metadata", {}).get("is_unpublished", True)
                if offer_id in missing_offer_ids and not is_unpublished:
                    missing_active_listings.append(offer_id)

            print(
                f"Removing {len(missing_active_listings)} listings that are missing from search but still active"
            )
            merged_data = [
                listing
                for listing in merged_data
                if listing.get("offer_id") not in missing_active_listings
            ]
            parsed_listings = [
                listing
                for listing in parsed_listings
                if listing.get("offer_id") not in missing_active_listings
            ]

            normalize_listings(parsed_listings)
            merged_data = merge_and_validate(
                merged_data, parsed_listings, "PARSED MERGE"
            )

    save_json_file(merged_data, json_file_path)

    print(f"Merged data saved: {len(merged_data)} total listings")

    print("Cleaning up shared VPN manager...")
    del shared_vpn
    gc.collect()

    return merged_data


if __name__ == "__main__":
    parse_data()
