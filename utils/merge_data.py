def merge_data(target_data, source_data):
    """Pure merge function - merges source data into target data with special logic"""

    def deep_merge(existing, new, is_new_offer=False):
        """Recursively merge nested dictionaries with special date handling"""
        # Extract updated_date for special logic
        updated_date = None
        if "metadata" in new and isinstance(new["metadata"], dict):
            updated_date = new["metadata"].get("updated_date")

        # Handle special cases if we have updated_date
        if updated_date:
            # Ensure metadata exists
            if "metadata" not in existing:
                existing["metadata"] = {}

            # Case 1: New offer - set publication_date
            if is_new_offer:
                existing["metadata"]["publication_date"] = updated_date

            # Case 2: Check if is_unpublished changed from false to true
            elif (
                "metadata" in new
                and new["metadata"].get("is_unpublished") is True
                and existing["metadata"].get("is_unpublished") is False
            ):
                existing["metadata"]["unpublished_date"] = updated_date

            # Case 3: Check if offer_price changed
            elif (
                "offer_price" in new
                and "offer_price" in existing
                and isinstance(new["offer_price"], (int, float))
                and isinstance(existing["offer_price"], (int, float))
                and new["offer_price"] != existing["offer_price"]
            ):

                # Initialize price_changes if it doesn't exist
                if "price_changes" not in existing:
                    existing["price_changes"] = []

                # Create new price change entry
                price_diff = int(new["offer_price"]) - int(existing["offer_price"])
                price_change = {
                    "change": str(price_diff),
                    "current_price": str(new["offer_price"]),
                    "previous_price": str(existing["offer_price"]),
                    "date": updated_date,
                }
                existing["price_changes"].append(price_change)

            # Always update last_active
            existing["metadata"]["last_active"] = updated_date

        # Regular merge for all fields except updated_date
        for key, value in new.items():
            # Skip updated_date - don't add it to merged data
            if key == "metadata" and isinstance(value, dict):
                # Handle metadata specially to exclude updated_date field
                if "metadata" not in existing:
                    existing["metadata"] = {}

                for meta_key, meta_value in value.items():
                    if meta_key not in ["updated_date"]:
                        if (
                            meta_key in existing["metadata"]
                            and isinstance(existing["metadata"][meta_key], dict)
                            and isinstance(meta_value, dict)
                        ):
                            deep_merge(existing["metadata"][meta_key], meta_value)
                        else:
                            existing["metadata"][meta_key] = meta_value
            elif (
                key in existing
                and isinstance(existing[key], dict)
                and isinstance(value, dict)
            ):
                # For nested dicts, recursively merge
                deep_merge(existing[key], value)
            else:
                # Skip updating offer_price and estimation_price fields
                if key in ["timestamp"]:
                    continue
                # Skip updating if new value is None
                if value is None:
                    continue
                # Skip updating description if offer is being unpublished
                if (
                    key == "description"
                    and "metadata" in new
                    and new["metadata"].get("is_unpublished") is True
                ):
                    continue
                # For non-dict values, always update from new source
                existing[key] = value

    # Create a dictionary for quick lookup of target offers by offer_id
    target_by_id = {listing["offer_id"]: listing for listing in target_data}

    # Merge source_data into target_data
    for item in source_data:
        if "offer_id" in item:
            offer_id = item["offer_id"]
            if offer_id in target_by_id:
                # Update existing item
                deep_merge(target_by_id[offer_id], item, is_new_offer=False)
            else:
                # Add new item - first do a deep copy to avoid modifying source
                import copy

                new_item = copy.deepcopy(item)

                # Apply special logic for new offers
                if (
                    "metadata" in new_item
                    and isinstance(new_item["metadata"], dict)
                    and "updated_date" in new_item["metadata"]
                ):

                    updated_date = new_item["metadata"]["updated_date"]
                    new_item["metadata"]["publication_date"] = updated_date
                    new_item["metadata"]["last_active"] = updated_date

                    # Remove updated_date (now that it's been used)
                    new_item["metadata"].pop("updated_date", None)

                target_by_id[offer_id] = new_item

    # Convert back to list
    return list(target_by_id.values())