"""Generate the reproducible CSV transaction log for the Spark lab."""

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path


# (category, relative frequency, minimum price in cents, maximum price in cents)
CATEGORIES = (
    ("groceries", 30, 250, 18_000),
    ("restaurants", 15, 500, 12_000),
    ("transport", 12, 150, 8_000),
    ("household", 10, 400, 25_000),
    ("clothing", 9, 800, 35_000),
    ("health", 8, 300, 20_000),
    ("books", 6, 500, 10_000),
    ("entertainment", 5, 400, 15_000),
    ("electronics", 3, 2_500, 120_000),
    ("travel", 2, 3_000, 180_000),
)

START_DATE = date(2026, 1, 1)
END_DATE = date(2026, 6, 30)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=Path("data/transactions.csv"),
        help="CSV file to create (default: data/transactions.csv)",
    )
    parser.add_argument(
        "--rows", type=int, default=8_500_000,
        help="number of transactions (default: 8500000)",
    )
    parser.add_argument(
        "--seed", type=int, default=2026,
        help="random seed for reproducible data (default: 2026)",
    )
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("--rows must be a positive integer")
    return args


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    day_count = (END_DATE - START_DATE).days + 1
    categories = [item[0] for item in CATEGORIES]
    weights = [item[1] for item in CATEGORIES]
    price_ranges = {item[0]: (item[2], item[3]) for item in CATEGORIES}

    # TextIOWrapper buffers writes, so memory use does not grow with row count.
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(("user_id", "timestamp", "category", "amount"))
        for _ in range(args.rows):
            user_id = rng.randint(1000, 9999)
            transaction_date = START_DATE + timedelta(days=rng.randrange(day_count))
            timestamp = (
                f"{transaction_date.isoformat()}T"
                f"{rng.randint(7, 22):02d}:{rng.randrange(60):02d}:"
                f"{rng.randrange(60):02d}"
            )
            category = rng.choices(categories, weights=weights, k=1)[0]
            minimum, maximum = price_ranges[category]
            cents = rng.randint(minimum, maximum)
            writer.writerow((user_id, timestamp, category, f"{cents // 100}.{cents % 100:02d}"))

    print(f"Created {args.rows:,} transactions in {args.output}")
    print(f"Dates: {START_DATE} to {END_DATE}; seed: {args.seed}; header included")


if __name__ == "__main__":
    main()
