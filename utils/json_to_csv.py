#!/usr/bin/env python3
import json
import csv
from utils.csv_fieldnames import fieldnames


def transform_listing(listing):
    """Transform a single listing to match the original CSV structure"""
    row = {}

    for field in fieldnames:
        row[field] = ""

    # Basic fields
    row["offer_id"] = listing.get("offer_id", "")
    row["title"] = listing.get("title", "")
    row["description"] = listing.get("description", "")
    row["distance"] = listing.get("distance", "")
    row["price_value"] = listing.get("offer_price", "")
    row["estimated_price"] = listing.get("estimated_price", "")
    row["offer_url"] = listing.get("offer_url", "")
    row["price_change_value"] = listing.get("price_change_value", "")
    row["price_change_date"] = listing.get("price_change_date", "")

    # Metadata fields
    metadata = listing.get("metadata", {})
    row["publication_date"] = metadata.get("publication_date", "")
    row["last_active"] = metadata.get("last_active", "")
    row["unpublished_date"] = metadata.get("unpublished_date", "")

    # Get pre-computed metadata fields
    row["total_views"] = metadata.get("total_views", "")
    row["today_views"] = metadata.get("today_views", "")
    row["unique_views"] = metadata.get("unique_views", "")
    row["status"] = metadata.get("status", "")

    # Geo fields
    geo = listing.get("geo", {})
    row["address"] = geo.get("address", "")
    row["district"] = geo.get("district", "")
    row["neighborhood"] = geo.get("neighborhood", "")
    row["metro_station"] = geo.get("metro_station", "")

    # Rental terms - ensure numeric consistency
    rental = listing.get("rental_terms", {})
    row["utilities_payment"] = rental.get("Оплата ЖКХ", "")
    row["security_deposit"] = rental.get("Залог", "")
    row["commission"] = rental.get("Комиссия", "")
    row["prepayment"] = rental.get("Предоплата", "")
    row["rental_period"] = rental.get("Срок аренды", "")
    row["living_conditions"] = rental.get("Условия проживания", "")

    # Apartment details
    apartment = listing.get("apartment", {})
    row["apartment_type"] = apartment.get("Тип жилья", "")
    row["total_area"] = apartment.get("Общая площадь", "")
    row["living_area"] = apartment.get("Жилая площадь", "")
    row["kitchen_area"] = apartment.get("Площадь кухни", "")
    row["bathroom"] = apartment.get("Санузел", "")
    row["renovation"] = apartment.get("Ремонт", "")
    row["balcony"] = apartment.get("Балкон/лоджия", "")
    row["layout"] = apartment.get("Планировка", "")
    row["ceiling_height"] = apartment.get("Высота потолков", "")
    row["view"] = apartment.get("Вид из окон", "")
    row["sleeping_places"] = apartment.get("Спальных мест", "")
    row["floor"] = apartment.get("floor", "")
    row["total_floors"] = apartment.get("total_floors", "")
    row["room_count"] = apartment.get("room_count", "")

    # Building details
    building = listing.get("building", {})
    row["elevators"] = building.get("Количество лифтов", "")
    row["parking"] = building.get("Парковка", "")
    row["year_built"] = building.get("Год постройки", "")
    row["building_type"] = building.get("Тип дома", "")
    row["building_series"] = building.get("Строительная серия", "")
    row["ceiling_type"] = building.get("Тип перекрытий", "")
    row["heating"] = building.get("Отопление", "")
    row["gas_supply"] = building.get("Газоснабжение", "")
    row["garbage_chute"] = building.get("Мусоропровод", "")
    row["emergency"] = building.get("Аварийность", "")
    row["entrances"] = building.get("Подъезды", "")

    # Features - set boolean fields
    features = listing.get("features", [])
    row["has_refrigerator"] = "Холодильник" in features
    row["has_dishwasher"] = "Посудомоечная машина" in features
    row["has_washing_machine"] = "Стиральная машина" in features
    row["has_air_conditioner"] = "Кондиционер" in features
    row["has_internet"] = "Интернет" in features
    row["has_bathtub"] = "Ванна" in features
    row["has_room_furniture"] = "Мебель в комнатах" in features
    row["has_tv"] = "Телевизор" in features
    row["has_kitchen_furniture"] = "Мебель на кухне" in features
    row["has_shower_cabin"] = "Душевая кабина" in features

    return row


def convert_json_to_csv(output_file, listings):
    """
    Convert JSON listings to CSV format.

    Args:
        output_file (str, optional): Path to output CSV file
        listings (list, optional): Pre-loaded listings data

    Returns:
        bool: True if successful, False otherwise
    """
    # Set default paths if not provided

    print(f"Using provided listings data: {len(listings)} listings")

    try:
        # Transform all listings to CSV format
        transformed_listings = []
        for listing in listings:
            transformed = transform_listing(listing)
            transformed_listings.append(transformed)

        # Write to CSV
        print(f"Writing CSV to: {output_file}")
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transformed_listings)

        print(f"✅ Successfully converted {len(transformed_listings)} listings to CSV")
        print(f"📄 Output file: {output_file}")
        return True

    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        return False


def main():
    """Main function for command line usage"""
    return convert_json_to_csv()


if __name__ == "__main__":
    main()
