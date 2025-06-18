import csv


def map_fields_to_csv():
    # Read CSV headers
    with open(
        "/Users/klim/Desktop/networks/cian/parse_page/data/combined_data.csv",
        "r",
        encoding="utf-8",
    ) as f:
        reader = csv.reader(f)
        csv_headers = next(reader)

    # Define mappings from Russian fields to CSV columns
    rental_terms_mappings = {
        "Залог": "security_deposit",
        "Комиссии": "commission",
        "Комиссия": "commission",
        "Оплата ЖКХ": "utilities_payment",
        "Предоплата": "prepayment",
        "Предоплаты": "prepayment",
        "Срок аренды": "rental_period",
        "Торг": "negotiable",
        "Условия проживания": "living_conditions",
    }

    apartment_mappings = {
        "Балкон/лоджия": "balcony",
        "Вид из окон": "view",
        "Высота потолков": "ceiling_height",
        "Год постройки": "year_built",
        "Жилая площадь": "living_area",
        "Комнат в аренду": "rooms_for_rent",
        "Комнат в квартире": "rooms_in_apartment",
        "Общая площадь": "total_area",
        "Планировка": "layout",
        "Площадь комнат": "room_area",
        "Площадь кухни": "kitchen_area",
        "Ремонт": "renovation",
        "Санузел": "bathroom",
        "Спальных мест": "sleeping_places",
        "Тип жилья": "apartment_type",
        "Этаж": "floor",
    }

    building_mappings = {
        "Аварийность": "emergency",
        "Газоснабжение": "gas_supply",
        "Год постройки": "year_built",
        "Залог": "security_deposit",
        "Количество лифтов": "elevators",
        "Мусоропровод": "garbage_chute",
        "Отопление": "heating",
        "Парковка": "parking",
        "Подъезды": "entrances",
        "Строительная серия": "building_series",
        "Тип дома": "building_type",
        "Тип перекрытий": "ceiling_type",
        "Условия проживания": "living_conditions",
    }

    features_mappings = {
        "Ванна": "has_bathtub",
        "Душевая кабина": "has_shower_cabin",
        "Интернет": "has_internet",
        "Кондиционер": "has_air_conditioner",
        "Мебель в комнатах": "has_room_furniture",
        "Мебель на кухне": "has_kitchen_furniture",
        "Посудомоечная машина": "has_dishwasher",
        "Стиральная машина": "has_washing_machine",
        "Телевизор": "has_tv",
        "Холодильник": "has_refrigerator",
    }

    # Write mapping results
    with open(
        "/Users/klim/Desktop/networks/cian/parse_page/scraper/field_csv_mappings.txt",
        "w",
        encoding="utf-8",
    ) as f:
        f.write("MAPPING RUSSIAN FIELDS TO CSV COLUMNS\n")
        f.write("=" * 50 + "\n\n")

        all_mappings = [
            ("RENTAL_TERMS", rental_terms_mappings),
            ("APARTMENT", apartment_mappings),
            ("BUILDING", building_mappings),
            ("FEATURES", features_mappings),
        ]

        for section_name, mappings in all_mappings:
            f.write(f"{section_name} MAPPINGS:\n")
            f.write("-" * 30 + "\n")

            for russian_field, csv_field in mappings.items():
                # Check if CSV field exists
                exists = csv_field in csv_headers
                status = "✓" if exists else "✗"
                f.write(f"{russian_field:<30} -> {csv_field:<25} {status}\n")
            f.write("\n")

        # Summary
        f.write("SUMMARY:\n")
        f.write("-" * 30 + "\n")

        total_mappings = 0
        found_mappings = 0
        missing_mappings = []

        for section_name, mappings in all_mappings:
            for russian_field, csv_field in mappings.items():
                total_mappings += 1
                if csv_field in csv_headers:
                    found_mappings += 1
                else:
                    missing_mappings.append(f"{russian_field} -> {csv_field}")

        f.write(f"Total mappings: {total_mappings}\n")
        f.write(f"Found in CSV: {found_mappings}\n")
        f.write(f"Missing from CSV: {len(missing_mappings)}\n")

        if missing_mappings:
            f.write(f"\nMISSING MAPPINGS:\n")
            for missing in missing_mappings:
                f.write(f"  {missing}\n")

    print(f"Field mappings written to field_csv_mappings.txt")
    print(f"Total mappings checked: {total_mappings}")
    print(f"Found in CSV: {found_mappings}")
    print(f"Missing from CSV: {len(missing_mappings)}")


if __name__ == "__main__":
    map_fields_to_csv()
