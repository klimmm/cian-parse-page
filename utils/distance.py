import requests
import time
import random
import re
import logging
import json


logger = logging.getLogger("distances")

max_retries = 5
timeout = 10
sleep_time = 2


def get_coordinates(address):
    """
    Get coordinates with comprehensive error handling for network issues and complex addresses.
    """
    headers = {"User-Agent": "PythonGeocoder/1.0"}

    # Create address variations from most specific to most general
    address_variations = list(
        dict.fromkeys(
            [
                address,
                re.sub(r"(?<=\s)вл(\d)", r"\1", address),
                # Keep only the numeric part of building numbers
                re.sub(r"(\d+)[А-Яа-я]+\d*", r"\1", address),
                # Simple street + main building number pattern
                re.sub(
                    r"(улица\s+[А-Яа-я]+|[А-Яа-я]+\s+переулок),\s+\d+[А-Яа-я]*.*",
                    r"\1",
                    address,
                ),
            ]
        )
    )

    # Try each address variant
    for addr_variant in address_variations:
        # Retry loop
        for attempt in range(max_retries):
            try:
                # Add timeout to prevent hanging requests
                params = {
                    "q": addr_variant,
                    "format": "json",
                    "countrycodes": "ru",
                    "addressdetails": 1,
                }

                # Sleep before making request
                time.sleep(1 + random.uniform(0, 0.5))

                response = requests.get(
                    "https://nominatim.openstreetmap.org/search",
                    params=params,
                    headers=headers,
                    timeout=timeout,
                )

                response.raise_for_status()
                data = response.json()

                if data:
                    lat = data[0]["lat"]
                    lon = data[0]["lon"]
                    if addr_variant != address:
                        logger.info(
                            f"Found coordinates using simplified address: '{addr_variant}' for '{address}'"
                        )
                    return float(lat), float(lon)

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error for '{addr_variant}', retrying in 2s: {e}")
                    time.sleep(sleep_time)
                else:
                    break

    raise Exception(f"No coordinates found for address after multiple attempts: {address}")


def calculate_distance(from_point, to_point):
    """
    Calculate walking distance between two coordinate points using OpenStreetMap's routing API.

    Parameters:
    from_point (tuple): Starting point coordinates as (lat, lon) tuple
    to_point (tuple): Ending point coordinates as (lat, lon) tuple

    Returns:
    float: Distance in kilometers
    """
    # Format coordinates as 'lon,lat' (note the order)
    from_coord = f"{from_point[1]},{from_point[0]}"
    to_coord = f"{to_point[1]},{to_point[0]}"

    # Only use OpenStreetMap endpoint for foot routing
    endpoint_url = f"https://routing.openstreetmap.de/routed-foot/route/v1/foot/{from_coord};{to_coord}"

    params = {
        "overview": "false",
        "alternatives": "false",
    }

    headers = {"User-Agent": "PythonGeocoder/1.0"}

    for attempt in range(max_retries):
        try:
            # Add timeout parameter to prevent hanging requests
            response = requests.get(
                endpoint_url, params=params, headers=headers, timeout=timeout
            )
            response.raise_for_status()
            data = response.json()
            distance_meters = data["routes"][0]["distance"]
            logger.info(f"OpenStreetMap walking distance: {distance_meters:.2f}m")
            return distance_meters

        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Error, retrying in 2s: {e}")
                time.sleep(sleep_time)
            else:
                raise

    raise Exception("All routing API attempts failed")

def save_distances_to_json(listings_data, json_file_path):
    """Save updated distances back to the JSON file"""
    try:
        print(f"💾 Saving updated distances to: {json_file_path}")
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(listings_data, f, indent=2, ensure_ascii=False)
        print("✅ Distances saved successfully!")
        return True
    except Exception as e:
        print(f"❌ Error saving distances to JSON: {e}")
        return False


def calculate_and_update_distances(
    listings_data,
    json_file_path,
    reference_address="Москва, переулок Большой Саввинский, 3",
    ref_coords=(55.7355742, 37.5701096)
):
    """Calculate distances for listings and update them in-place"""
    # Calculate reference coordinates once
    print("\n📏 Calculating distances for listings...")
    for listing in listings_data:

        if not listing.get("distance", ""):
            try:
                # Get address from the listing
                geo = listing.get("geo", {})
                full_address = geo.get("address", "")
                to_point_coords = get_coordinates(full_address)
                distance_meters = calculate_distance(
                    from_point=ref_coords, to_point=to_point_coords
                )
                if distance_meters is not None:
                    distance_km = distance_meters / 1000
                    listing["distance"] = round(distance_km, 2)
            except Exception as e:
                print(
                    f"Failed to calculate distance for {listing.get('offer_id', 'unknown')}: {e}"
                )

    # Save updated distances to JSON file
    print("\n💾 Saving updated distances to JSON...")
    save_distances_to_json(listings_data, json_file_path)
  