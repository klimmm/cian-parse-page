import os
from datetime import datetime, timedelta
import re
import yaml
import math
import json

def parse_russian_date(time_label):
    """Parse Russian time labels to YYYY-MM-DD HH:MM:SS format"""
    if not time_label:
        return None

    now = datetime.now()
    months = {
        "янв": 1,
        "фев": 2,
        "мар": 3,
        "апр": 4,
        "май": 5,
        "мая": 5,
        "июн": 6,
        "июл": 7,
        "авг": 8,
        "сен": 9,
        "окт": 10,
        "ноя": 11,
        "дек": 12
    }

    try:
        # Pattern 1: "сегодня, HH:MM"
        if "сегодня" in time_label:
            match = re.search(r"(\d{1,2}):(\d{2})", time_label)
            if match:
                hour, minute = int(match.group(1)), int(match.group(2))
                result = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                return result.strftime("%Y-%m-%d %H:%M:%S")

        # Pattern 2: "вчера, HH:MM"
        elif "вчера" in time_label:
            match = re.search(r"(\d{1,2}):(\d{2})", time_label)
            if match:
                hour, minute = int(match.group(1)), int(match.group(2))
                result = now - timedelta(days=1)
                result = result.replace(
                    hour=hour, minute=minute, second=0, microsecond=0
                )
                return result.strftime("%Y-%m-%d %H:%M:%S")

        # Pattern 3: "DD месяц, HH:MM"
        else:
            match = re.search(
                r"(\d{1,2})\s+([а-яА-Я]+),?\s+(\d{1,2}):(\d{2})", time_label
            )
            if match:
                day = int(match.group(1))
                month_name = match.group(2).lower()
                hour = int(match.group(3))
                minute = int(match.group(4))

                if month_name in months:
                    month = months[month_name]
                    year = now.year

                    result = datetime(year, month, day, hour, minute, 0)

                    if result > now:
                        result = result.replace(year=year - 1)

                    return result.strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        print(f"Error parsing time label '{time_label}': {e}")

    return time_label


def construct_search_url(config):

    base_url = "https://cian.ru"

    url = f"{base_url}/cat.php?currency=2&engine_version=2&type=4&deal_type=rent&sort=creation_date_desc&"

    for key in config:
        if key == "district":
            if config["district"]:
                for i, district in enumerate(config["district"]):
                    url += f"district[{i}]={district}&"
        elif key == "street":
            if config["street"]:
                for i, street in enumerate(config["street"]):
                    url += f"street[{i}]={street}&"
        elif key == "metro":
            if config["metro"]:
                for i, metro in enumerate(config["metro"]):
                    url += f"metro[{i}]={metro}&"
        elif key == "rooms":
            if config["rooms"]:
                for room in config["rooms"]:
                    url += f"room{room}=1&"
        else:
            url += f"{key}={config[key]}&"

    return url.rstrip("&")


def construct_offer_url(offer_id):
    """Construct offer URL using environment variable"""

    base_url = os.getenv("BASE_URL")

    return f"{base_url}/rent/flat/{offer_id}/"


def load_json_file(filename, default=None):
    """Safely load a JSON file with proper error handling"""
    if default is None:
        default = [] if filename.endswith("listings.json") else {}

    try:
        with open(filename, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def load_yaml_file(filename):
    """Load YAML configuration file

    Args:
        filename: Path to YAML file

    Returns:
        Parsed YAML data
    """
    with open(filename, "r") as f:
        return yaml.safe_load(f)


def save_json_file(data, filename, ensure_ascii=False, indent=2):
    """Save data to JSON file with proper formatting

    Args:
        data: Data to save
        filename: Output filename
        ensure_ascii: Whether to ensure ASCII encoding (default: False)
        indent: JSON indentation (default: 2)
    """
    with open(filename, "w") as f:
        json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii)


def generate_search_page_urls(base_url, search_summary, listings_per_page=28):
    """Generate paginated search URLs based on total listings count

    Args:
        base_url: Base search URL
        total_listings: Total number of listings found
        listings_per_page: Number of listings per page (default: 28)

    Returns:
        List of paginated URLs
    """
    if search_summary and "listings" in search_summary[0]:
        total_listings = search_summary[0]["listings"]
        print(f"Found total listings: {total_listings}")
    else:
        print("Could not extract total listings count")
        print("Using default 16 pages as fallback")
        total_listings = 16 * 28

    total_pages = math.ceil(total_listings / listings_per_page)
    return [f"{base_url}&p={i+1}" for i in range(total_pages)]


def generate_listing_page_urls(offer_ids):
    """Generate listing detail page URLs from offer IDs

    Args:
        offer_ids: List or set of offer IDs

    Returns:
        List of listing page URLs
    """
    return [f"https://www.cian.ru/rent/flat/{offer_id}" for offer_id in offer_ids]




def flatten_search_results(search_results):
    """
    Flatten nested search results into a single list.

    Args:
        search_results: List of result objects containing search_results

    Returns:
        Flattened list of all offers
    """
    flattened_listings = []

    for result in search_results:
        flattened_listings.extend(result["search_results"])

    return flattened_listings


def dedupe_listings(listings, key="offer_id"):
    """
    Remove duplicate listings based on a key.

    Args:
        listings: List of offer objects
        key: The dictionary key to use for deduplication

    Returns:
        List of unique offer objects
    """
    unique_keys = set()
    unique_listings = []

    for listing in listings:
        if listing[key] not in unique_keys:
            unique_listings.append(listing)
            unique_keys.add(listing[key])

    return unique_listings