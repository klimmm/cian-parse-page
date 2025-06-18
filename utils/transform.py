#!/usr/bin/env python3
import re
from datetime import datetime


def parse_view_stats(stats_text):
    """Parse view statistics from text like 'total, today, unique'"""
    if not stats_text:
        return "", "", ""

    parts = stats_text.split(", ")

    numeric_values = []
    for part in parts:
        match = re.search(r"(\d+)", part)
        if match:
            numeric_values.append(match.group(1))

    # Assign based on position: total_views, today_views, unique_views
    total_views = numeric_values[0] if len(numeric_values) >= 1 else ""
    today_views = numeric_values[1] if len(numeric_values) >= 2 else ""
    unique_views = numeric_values[2] if len(numeric_values) >= 3 else ""

    return total_views, today_views, unique_views


def parse_floor_info(floor_text):
    """Parse floor info from text like '1 из 13'"""
    if not floor_text or " из " not in floor_text:
        return "", ""

    parts = floor_text.split(" из ")
    return parts[0], parts[1]


def extract_room_count(title):
    """Extract room count from title like '1-комн. квартира'"""
    if not title:
        return ""

    match = re.search(r"(\d+)-комн\.", title)
    if match:
        return match.group(1)

    if "студия" in title.lower():
        return "0"

    return ""


def extract_address_components(geo_data):
    """Extract district and neighborhood from address items"""
    district = ""
    neighborhood = ""

    if "address_items" in geo_data:
        for item in geo_data["address_items"]:
            text = item.get("text", "")
            if "АО" in text:
                district = text
            elif "р-н" in text:
                neighborhood = text

    return district, neighborhood


def find_most_recent_price_change_info(listing):
    """Find the most recent price change value and date from price_changes list

    Returns:
        tuple: (change_value, change_date) where both are strings
    """
    price_changes = listing.get("price_changes", [])
    if not price_changes or not isinstance(price_changes, list):
        return "", ""

    most_recent_change = None
    most_recent_date = None

    for change in price_changes:
        if isinstance(change, dict) and "date" in change and "change" in change:
            try:
                change_date = datetime.strptime(
                    str(change["date"]), "%Y-%m-%d %H:%M:%S"
                )
                if most_recent_date is None or change_date > most_recent_date:
                    most_recent_date = change_date
                    most_recent_change = change["change"]
            except (ValueError, TypeError):
                continue

    change_value = most_recent_change if most_recent_change is not None else ""
    change_date = most_recent_date.strftime("%Y-%m-%d %H:%M:%S") if most_recent_date else ""

    return change_value, change_date


def extract_metro_station(geo):
    """Extract the first metro station name from geo data"""
    metro_stations = geo.get("metro_stations", [])
    if metro_stations:
        return metro_stations[0].get("name", "")
    return ""


def transform_listings_data(listings_data):
    """Apply transformation functions to create new fields for all listings"""
    print("\n🔄 Transforming listings data...")

    for listing in listings_data:
        try:
            # Rename fields for CSV output consistency
            if "estimation_price" in listing:
                listing["estimated_price"] = listing.pop("estimation_price")
            if "url" in listing:
                listing["offer_url"] = listing.pop("url")
            
            # Extract and add price change information to listing level
            change_value, change_date = find_most_recent_price_change_info(listing)
            listing["price_change_value"] = change_value
            listing["price_change_date"] = change_date

            # Work with geo data
            geo = listing.get("geo", {})
            if geo:
                # Rename full_address to address for CSV output
                if "full_address" in geo:
                    geo["address"] = geo.pop("full_address")
                
                # Extract and add address components to geo
                district, neighborhood = extract_address_components(geo)
                geo["district"] = district
                geo["neighborhood"] = neighborhood

                # Extract and add metro station to geo
                geo["metro_station"] = extract_metro_station(geo)

            # Work with metadata
            metadata = listing.get("metadata", {})
            if metadata:
                # Parse and add view statistics to metadata
                offer_stats = metadata.get("offer_stats", "")
                total_views, today_views, unique_views = parse_view_stats(offer_stats)
                metadata["total_views"] = total_views
                metadata["today_views"] = today_views
                metadata["unique_views"] = unique_views

                # Set status based on unpublished flag in metadata
                is_unpublished = metadata.get("is_unpublished", False)
                metadata["status"] = "non active" if is_unpublished else "active"

            # Work with apartment data
            apartment = listing.get("apartment", {})
            if apartment:
                # Parse and add floor information to apartment
                floor_info = apartment.get("Этаж", "")
                floor, total_floors = parse_floor_info(floor_info)
                apartment["floor"] = int(floor) if floor.isdigit() else floor
                apartment["total_floors"] = int(total_floors) if total_floors.isdigit() else total_floors

                # Extract and add room count from title to apartment
                title = listing.get("title") or ""
                room_count = extract_room_count(title)
                apartment["room_count"] = int(room_count) if room_count.isdigit() else room_count

        except Exception as e:
            print(
                f"Error transforming listing {listing.get('offer_id', 'unknown')}: {e}"
            )

    print("✅ Listings transformation completed!")