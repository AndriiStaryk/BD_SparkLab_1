"""Part 3: same action as Part 1; master and split size come from spark-submit."""

import argparse

from part1 import run
from pipeline import DEFAULT_INPUT


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--app-name", required=True)
    args = parser.parse_args()
    run(args.app_name, args.input)
