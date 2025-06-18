import re
from utils.helpers import parse_russian_date
import urllib.parse


def parse_value(value, default=None):
    """Universal parser for numeric values with standardized replacements"""
    # Handle empty or non-string values
    if not value:
        return default
    if isinstance(value, (int, float)):
        return value
    if not isinstance(value, str):
        return default

    try:
        # Apply all standard replacements
        cleaned = value.strip()

        # Standard replacements for all types
        replacements = [
            ("₽", ""),
            ("/мес", ""),
            (".", ""),
            ("м²", ""),
            ("м", ""),
            (",", "."),
        ]

        for old, new in replacements:
            cleaned = cleaned.replace(old, new)

        # Remove all spaces
        cleaned = re.sub(r"\s+", "", cleaned)

        # Handle special cases
        if cleaned.lower() == "нет":
            return 0.0

        # Handle percentage format
        if cleaned.endswith("%"):
            cleaned = cleaned[:-1]

        # Try to convert to float first
        try:
            result = float(cleaned)
            # Convert to int if it's a whole number
            return int(result) if result.is_integer() else result
        except ValueError:
            # If not a direct conversion, try to extract any numeric value
            match = re.search(r"(\d+(?:\.\d+)?)", cleaned)
            if match:
                result = float(match.group(1))
                return int(result) if result.is_integer() else result

        return default
    except Exception as e:
        print(f"Error parsing '{value}': {e}")
        return default


def extract_id_from_href(href):
    """Extract ID from href URL parameter"""
    if not href:
        return None

    try:
        decoded_href = urllib.parse.unquote(href)

        # Define patterns to match different URL formats
        patterns = [
            r"district\[0\]=(\d+)",
            r"metro\[0\]=(\d+)",
            r"street\[0\]=(\d+)",
            r"house\[0\]=(\d+)",
            r"region=(\d+)",
            r"/dom/[^/]+-(\d+)/?$",
        ]

        # Try each pattern
        for pattern in patterns:
            match = re.search(pattern, decoded_href)
            if match:
                return match.group(1)
    except Exception as e:
        print(f"Error extracting ID from href '{href}': {e}")

    return None


def clean_metro_from_address(address):
    """Remove metro station references from address strings"""
    if not address or not isinstance(address, str):
        return address

    # Remove metro references
    cleaned = re.sub(
        r"(?:^м\.\s*[^,]+,\s*|,\s*м\.\s*[^,]+(?:,|$))",
        ", ",
        address,
        flags=re.IGNORECASE,
    )

    # Clean up formatting
    cleaned = re.sub(r",\s*,", ",", cleaned)
    cleaned = re.sub(r"^,\s*|,\s*$", "", cleaned)

    return cleaned.strip()


def extract_street_address(address_texts):
    """Extract street address from full address (remove city and district parts)"""
    return ", ".join(
        part for part in address_texts if not any(x in part for x in ["АО", "р-н"])
    )


def normalize_street_names(text):
    """Convert full street names to abbreviated forms"""
    if not text or not isinstance(text, str):
        return text

    # Define replacements
    replacements = [
        ("улица", "ул."),
        ("шоссе", "ш."),
        ("проспект", "просп."),
        ("переулок", "пер."),
        ("бульвар", "бул."),
        ("набережная", "наб."),
    ]

    # Apply replacements
    for long_form, short_form in replacements:
        text = re.sub(r"\b" + re.escape(long_form) + r"\b", short_form, text)

    return text


def normalize_listings(offers):
    """Normalize offer data after parsing (modifies in-place)"""
    for offer in offers:
        # Ensure required structures exist
        if "metadata" not in offer:
            offer["metadata"] = {}
        if "geo" not in offer:
            offer["geo"] = {}
        if "apartment" not in offer:
            offer["apartment"] = {}
        if "rental_terms" not in offer:
            offer["rental_terms"] = {}

        # Normalize IDs and dates
        if "offer_id" in offer:
            offer["offer_id"] = str(offer["offer_id"])

        metadata = offer["metadata"]
        if "updated_date" in metadata and metadata["updated_date"]:
            metadata["updated_date"] = parse_russian_date(metadata["updated_date"])

        # Clear unpublished_date if not unpublished
        if not metadata.get("is_unpublished", False):
            metadata["unpublished_date"] = ""

        # Parse numeric fields
        for field in ["offer_price", "estimation_price"]:
            if field in offer and offer[field]:
                offer[field] = parse_value(offer[field])

        # Normalize URL format
        if "url" in offer and offer["url"] and offer["url"].endswith("/"):
            offer["url"] = offer["url"].rstrip("/")

        # Format description for CSV compatibility
        if "description" in offer and offer["description"]:
            offer["description"] = offer["description"].replace("\n", " ")

        # Convert image_urls to list format
        if "image_urls" in offer:
            image_urls = offer["image_urls"]
            if isinstance(image_urls, str) and image_urls:
                offer["image_urls"] = [
                    url.strip() for url in image_urls.split(",") if url.strip()
                ]
            elif not image_urls:
                offer["image_urls"] = []

        # Clean and normalize geo data
        geo = offer["geo"]
        if "address_items" in geo and isinstance(geo["address_items"], list):
            address_texts = []

            for item in geo["address_items"]:
                if isinstance(item, dict):
                    # Extract ID from href
                    if "href" in item:
                        item["id"] = extract_id_from_href(item["href"])
                        item.pop("href", None)

                    # Normalize street names
                    if "text" in item:
                        item["text"] = normalize_street_names(item["text"])
                        address_texts.append(item["text"])

            if address_texts:
                geo["full_address"] = extract_street_address(address_texts)

        # Normalize apartment fields
        apartment = offer["apartment"]
        for field in [
            "Общая площадь",
            "Жилая площадь",
            "Площадь кухни",
            "Высота потолков",
        ]:
            if field in apartment and apartment[field]:
                apartment[field] = parse_value(apartment[field])

        # Normalize rental terms
        rental_terms = offer["rental_terms"]
        
        # Standardize field names (migrate old variants to current names)
        if "Комиссии" in rental_terms and "Комиссия" not in rental_terms:
            rental_terms["Комиссия"] = rental_terms.pop("Комиссии")
        if "Предоплаты" in rental_terms and "Предоплата" not in rental_terms:
            rental_terms["Предоплата"] = rental_terms.pop("Предоплаты")
        if "Залога" in rental_terms and "Залог" not in rental_terms:
            rental_terms["Залог"] = rental_terms.pop("Залога")
        
        for field in ["Залог", "Комиссия", "Предоплата"]:
            if field in rental_terms and rental_terms[field]:
                rental_terms[field] = parse_value(rental_terms[field])

    return offers
