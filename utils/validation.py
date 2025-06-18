import json
from datetime import datetime


def deep_merge_with_logging(existing, new, path="", log_file=None):
    """Deep merge with detailed logging for validation purposes - logs only changes"""
    for key, value in new.items():
        current_path = f"{path}.{key}" if path else key

        if (
            key in existing
            and isinstance(existing[key], dict)
            and isinstance(value, dict)
        ):
            # Recursively merge nested dictionaries
            deep_merge_with_logging(existing[key], value, current_path, log_file)
        else:
            # Log only changes and additions
            if key in existing:
                if existing[key] != value:
                    message = (
                        f"    Override {current_path}: '{existing[key]}' -> '{value}'"
                    )
                    print(message)
                    if log_file:
                        log_file.write(message + "\n")
                # Skip logging unchanged values
            else:
                message = f"    Added {current_path}: '{value}'"
                print(message)
                if log_file:
                    log_file.write(message + "\n")

            # Update or add the field
            existing[key] = value


def validate_merge(existing_data, source_data, merged_data, stage_name="MERGE"):
    """Validate and analyze single merge operation with detailed logging"""
    print(
        f"\n🔍 Running ENHANCED validation for {stage_name} (v2.0 with change detection)..."
    )

    # Use a fixed filename for the current run
    log_filename = "utils/merge_analysis.txt"

    # Use write mode for first merge, append for subsequent
    mode = "w" if stage_name == "SEARCH MERGE" else "a"

    with open(log_filename, mode, encoding="utf-8") as log_file:
        log_file.write(
            f"{stage_name} ANALYSIS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        log_file.write("=" * 50 + "\n\n")
        log_file.write("VALIDATION VERSION: 2.0 (with actual change detection)\n\n")

        # Create ORIGINAL lookup dictionaries for analysis - deep copy to preserve original state
        import copy

        existing_by_id = {
            listing["offer_id"]: copy.deepcopy(listing) for listing in existing_data
        }
        merged_by_id = {listing["offer_id"]: listing for listing in merged_data}
        source_by_id = {
            item["offer_id"]: item for item in source_data if "offer_id" in item
        }

        # Find source data offer IDs
        source_offer_ids = set()
        if source_data:
            source_offer_ids = {
                item["offer_id"] for item in source_data if "offer_id" in item
            }

        existing_offer_ids = set(existing_by_id.keys())

        log_file.write(f"Existing data: {len(existing_offer_ids)} total\n")
        log_file.write(f"Source data: {len(source_offer_ids)} total\n")
        log_file.write(f"Merged data: {len(merged_data)} total\n")

        # Show intersections
        overlapping_offers = source_offer_ids & existing_offer_ids
        log_file.write(f"Overlapping offers: {len(overlapping_offers)}\n")

        # Select sample offers for detailed analysis - prioritize offers that actually exist
        sample_offer_ids = []
        changes_detected = []

        # First pass: find offers that actually have changes
        for item in source_data[:10]:  # Check first 10 offers
            if "offer_id" in item:
                offer_id = item["offer_id"]
                if offer_id in existing_by_id:
                    # Quick check for changes
                    existing_item = existing_by_id[offer_id]
                    merged_item = merged_by_id[offer_id]

                    # Check if any fields actually changed
                    existing_keys = set(existing_item.keys())
                    merged_keys = set(merged_item.keys())

                    # Look for key differences or value changes
                    if existing_keys != merged_keys:
                        changes_detected.append(offer_id)
                    else:
                        # Check for value changes in common fields
                        for key in existing_keys:
                            if str(existing_item[key]) != str(merged_item[key]):
                                changes_detected.append(offer_id)
                                break

        # Select samples: prioritize offers with changes, fallback to overlapping
        if changes_detected:
            sample_offer_ids = changes_detected[:3]
            log_file.write(f"Found {len(changes_detected)} offers with changes\n")
        else:
            sample_offer_ids = list(overlapping_offers)[:3]
            log_file.write(
                f"No changes detected in sample, showing overlapping offers\n"
            )

        log_file.write(f"Selected for detailed analysis: {sample_offer_ids}\n\n")


        # Additional analysis: show summary of all changes
        log_file.write(f"\nSUMMARY OF ALL CHANGES\n")
        log_file.write("-" * 20 + "\n")

        total_changes = 0
        field_change_count = {}
        offers_with_changes = 0

        # Compare ORIGINAL vs FINAL state for ALL offers
        log_file.write(f"\nDETAILED CHANGES BY OFFER\n")
        log_file.write("-" * 30 + "\n")
        
        for offer_id in source_offer_ids:
            if offer_id in existing_by_id and offer_id in merged_by_id:
                original_item = existing_by_id[offer_id]
                merged_item = merged_by_id[offer_id]
                offer_has_changes = False
                offer_changes = []

                # Check all fields in merged data
                for key in merged_item.keys():
                    if isinstance(merged_item[key], dict) and key in original_item and isinstance(original_item[key], dict):
                        # Check nested fields
                        for nested_key in merged_item[key].keys():
                            original_val = original_item[key].get(nested_key, "NOT_PRESENT")
                            merged_val = merged_item[key][nested_key]
                            if str(original_val) != str(merged_val):
                                total_changes += 1
                                offer_has_changes = True
                                field_name = f"{key}.{nested_key}"
                                field_change_count[field_name] = (
                                    field_change_count.get(field_name, 0) + 1
                                )
                                # Truncate long values for readability
                                orig_truncated = str(original_val)[:55] + "..." if len(str(original_val)) > 55 else str(original_val)
                                merged_truncated = str(merged_val)[:55] + "..." if len(str(merged_val)) > 55 else str(merged_val)
                                offer_changes.append(f"  {field_name}: '{orig_truncated}' → '{merged_truncated}'")
                    else:
                        # Regular field - compare field by field consistently
                        original_val = original_item.get(key, "NOT_PRESENT")
                        merged_val = merged_item[key]
                        if str(original_val) != str(merged_val):
                            total_changes += 1
                            offer_has_changes = True
                            field_change_count[key] = (
                                field_change_count.get(key, 0) + 1
                            )
                            # Truncate long values for readability
                            orig_truncated = str(original_val)[:55] + "..." if len(str(original_val)) > 55 else str(original_val)
                            merged_truncated = str(merged_val)[:55] + "..." if len(str(merged_val)) > 55 else str(merged_val)
                            offer_changes.append(f"  {key}: '{orig_truncated}' → '{merged_truncated}'")

                if offer_has_changes:
                    offers_with_changes += 1
                    log_file.write(f"\nOffer {offer_id} ({len(offer_changes)} changes):\n")
                    for change in offer_changes:
                        log_file.write(f"{change}\n")

        log_file.write(
            f"Total offers with changes: {offers_with_changes} out of {len(overlapping_offers)}\n"
        )
        log_file.write(f"Total field changes detected: {total_changes}\n")
        if field_change_count:
            log_file.write("\nField change breakdown:\n")
            for field, count in sorted(
                field_change_count.items(), key=lambda x: x[1], reverse=True
            ):
                log_file.write(f"  {field}: {count} changes\n")

            # Show most commonly changed fields
            if len(field_change_count) > 5:
                log_file.write("\nMost frequently changed fields:\n")
                top_fields = sorted(
                    field_change_count.items(), key=lambda x: x[1], reverse=True
                )[:5]
                for field, count in top_fields:
                    percentage = (count / len(overlapping_offers)) * 100
                    log_file.write(
                        f"  {field}: {count} changes ({percentage:.1f}% of offers)\n"
                    )
        else:
            log_file.write("No field-level changes detected\n")

    # Final validation summary
    print(f"{stage_name} validation:")
    print(f"  Existing data: {len(existing_data)} listings")
    print(f"  Source data: {len(source_data)} listings")
    print(f"  Final merged data: {len(merged_data)} listings")

    # Check detailed merge validation
    updated_count = 0
    new_count = 0

    for item in source_data:
        if "offer_id" in item:
            offer_id = item["offer_id"]
            if offer_id in existing_by_id:
                updated_count += 1
                # if updated_count <= 2:  # Show details for first 2 updated items
                #     original = existing_by_id[offer_id]
                #     merged_item = merged_by_id[offer_id]

                #     print(f"  Sample merge for {offer_id}:")
                #     print(f"    Original keys: {set(original.keys())}")
                #     print(f"    Source keys: {set(item.keys())}")
                #     print(f"    Merged keys: {set(merged_item.keys())}")

                #     # Check if existing-only fields were preserved
                #     original_only = set(original.keys()) - set(item.keys())
                #     if original_only:
                #         preserved = all(key in merged_item for key in original_only)
                #         print(
                #             f"    Preserved existing-only fields {original_only}: {preserved}"
                #         )

                #     # Check nested fields preservation
                #     for field_name in [
                #         "metadata",
                #         "geo",
                #         "apartment",
                #         "building",
                #         "rental_terms",
                #     ]:
                #         if field_name in original and field_name in item:
                #             if isinstance(original[field_name], dict) and isinstance(
                #                 item[field_name], dict
                #             ):
                #                 orig_keys = set(original[field_name].keys())
                #                 source_keys = set(item[field_name].keys())
                #                 merged_keys = set(merged_item[field_name].keys())

                #                 # Only show details if there are existing-only fields
                #                 existing_only = orig_keys - source_keys
                #                 if existing_only:
                #                     preserved = all(
                #                         key in merged_item[field_name]
                #                         for key in existing_only
                #                     )
                #                     print(
                #                         f"    {field_name} - Preserved existing-only fields {existing_only}: {preserved}"
                #                     )
            else:
                new_count += 1

    print(f"  Updated existing listings: {updated_count}")
    print(f"  New listings added: {new_count}")
