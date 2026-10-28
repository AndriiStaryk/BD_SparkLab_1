"""RDD-only version of the two transaction MapReduce jobs."""

from operator import add

HEADER = "user_id,timestamp,category,amount"
DEFAULT_INPUT = "file:///lab/data/transactions.csv"


def is_transaction(line):
    return line != HEADER


def parse_transaction(line):
    user_id, timestamp, category, amount = line.split(",", 3)
    if not user_id or not category or len(timestamp) < 10:
        raise ValueError(f"Invalid transaction: {line[:100]}")
    whole, fractional = amount.split(".")
    cents = int(whole) * 100 + int(fractional)
    return ((timestamp[:10], category), cents)


def parse_transaction_broken(line):
    """Intentional failure for Part 2: category is not a number."""
    user_id, timestamp, category, amount = line.split(",", 3)
    cents = int(category)  # The error runs on an executor, at the action.
    return ((timestamp[:10], category), cents)


def parsed_pairs(sc, input_path=DEFAULT_INPUT, parser=parse_transaction):
    lines = sc.textFile(input_path)
    pairs = lines.filter(is_transaction).map(parser)
    return lines, pairs


def daily_totals(pairs):
    # Task 1: ((date, category), total cents).
    return pairs.reduceByKey(add)


def category_row(item):
    (date, category), cents = item
    return category, (cents, 1)


def add_category_stats(left, right):
    return (left[0] + right[0], left[1] + right[1])


def leader_row(item):
    category, (cents, daily_record_count) = item
    return category, cents, daily_record_count


def merge_leaders(left, right):
    # Associative: winner by amount, then alphabetically; count all Task-1 rows.
    winner = left if (-left[1], left[0]) <= (-right[1], right[0]) else right
    return winner[0], winner[1], left[2] + right[2]


def build_pipeline(sc, input_path=DEFAULT_INPUT):
    lines, pairs = parsed_pairs(sc, input_path)
    daily = daily_totals(pairs)
    categories = daily.map(category_row).reduceByKey(add_category_stats)
    leader = categories.map(leader_row)
    return lines, leader


def money(cents):
    return f"{cents // 100}.{cents % 100:02d}"
