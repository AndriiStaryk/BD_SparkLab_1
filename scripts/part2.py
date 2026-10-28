"""Part 2: laziness, an intentional parser error, repeated reads and cache."""

import argparse
import json
import statistics
import time
import traceback

from pyspark import SparkConf, SparkContext

from pipeline import (
    DEFAULT_INPUT, build_pipeline, daily_totals, is_transaction,
    merge_leaders, parse_transaction_broken, parsed_pairs,
)


def timing(sc, input_path):
    build_times = []
    action_times = []
    for index in range(4):
        label = "warmup" if index == 0 else f"repeat-{index}"
        start = time.perf_counter()
        lines, leader = build_pipeline(sc, input_path)
        build_s = time.perf_counter() - start
        sc.setJobGroup(f"timing-{label}", f"Part2 timing {label} {input_path}")
        start = time.perf_counter()
        result = leader.reduce(merge_leaders)
        action_s = time.perf_counter() - start
        if index:
            build_times.append(build_s)
            action_times.append(action_s)
        print(json.dumps({"run": label, "partitions": lines.getNumPartitions(),
                          "build_s": round(build_s, 6),
                          "action_s": round(action_s, 6),
                          "leader": result[0], "task1_records": result[2]}), flush=True)
    print(f"BUILD_MEDIAN_S={statistics.median(build_times):.6f}", flush=True)
    print(f"ACTION_MEDIAN_S={statistics.median(action_times):.6f}", flush=True)


def failure(sc, input_path):
    lines = sc.textFile(input_path)
    pairs = lines.filter(is_transaction).map(parse_transaction_broken)
    print("BROKEN_TRANSFORMATION_BUILT; parsing has not run yet", flush=True)
    sc.setJobGroup("intentional-error", f"Part2 intentional parser error {input_path}")
    try:
        pairs.count()  # The exception reaches the driver here.
    except Exception:
        print("EXPECTED_EXCEPTION_AT=pairs.count()", flush=True)
        traceback.print_exc()
    else:
        raise AssertionError("The deliberately broken parser did not fail")


def action_pair(sc, pairs, label, input_path):
    sc.setJobGroup(f"{label}-count", f"Part2 {label} parsed count {input_path}")
    first_start = time.perf_counter()
    transaction_count = pairs.count()
    first_s = time.perf_counter() - first_start

    sc.setJobGroup(f"{label}-daily", f"Part2 {label} daily count {input_path}")
    second_start = time.perf_counter()
    daily_count = daily_totals(pairs).count()
    second_s = time.perf_counter() - second_start
    return {"transactions": transaction_count, "daily_records": daily_count,
            "count_s": round(first_s, 6), "daily_s": round(second_s, 6),
            "pair_s": round(first_s + second_s, 6)}


def reads(sc, input_path, use_cache, hold_seconds=0):
    cold_times = []
    warm_times = []
    expected = None
    for index in range(4):
        label = "warmup" if index == 0 else f"repeat-{index}"
        _, pairs = parsed_pairs(sc, input_path)
        if use_cache:
            pairs.cache()  # A declaration; the next count fills the cache.
        try:
            first = action_pair(sc, pairs, f"{label}-first", input_path)
            if expected is not None and (first["transactions"], first["daily_records"]) != expected:
                raise AssertionError("The repeated RDD result changed")
            expected = first["transactions"], first["daily_records"]
            print(json.dumps({"run": label, "phase": "first", **first}), flush=True)
            if use_cache:
                # Same RDD, now with populated cached partitions.
                warm = action_pair(sc, pairs, f"{label}-cached", input_path)
                print(json.dumps({"run": label, "phase": "cached", **warm}), flush=True)
                if (warm["transactions"], warm["daily_records"]) != expected:
                    raise AssertionError("Cached result changed")
            if index:
                cold_times.append(first["pair_s"])
                if use_cache:
                    warm_times.append(warm["pair_s"])
        finally:
            if use_cache and index < 3:
                pairs.unpersist(blocking=True)
    print(f"FIRST_PAIR_MEDIAN_S={statistics.median(cold_times):.6f}", flush=True)
    if use_cache:
        print(f"CACHED_PAIR_MEDIAN_S={statistics.median(warm_times):.6f}", flush=True)
        for info in sc._jsc.sc().getRDDStorageInfo():
            print(json.dumps({"cached_rdd_id": info.id(),
                              "cached_partitions": info.numCachedPartitions(),
                              "memory_bytes": info.memSize(),
                              "disk_bytes": info.diskSize()}), flush=True)
        if hold_seconds:
            print(f"LIVE_STORAGE_UI=http://localhost:4040/storage/; hold={hold_seconds}s",
                  flush=True)
            time.sleep(hold_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--mode", choices=("timing", "failure", "nocache", "cache"), required=True)
    parser.add_argument("--hold-seconds", type=int, default=0)
    args = parser.parse_args()
    if args.hold_seconds < 0 or (args.hold_seconds and args.mode != "cache"):
        parser.error("--hold-seconds must be non-negative and is only for cache mode")
    sc = SparkContext(conf=SparkConf().setAppName(f"SparkLab-Part2-{args.mode}"))
    sc.setLogLevel("WARN")
    try:
        print(f"INPUT={args.input}", flush=True)
        if args.mode == "timing":
            timing(sc, args.input)
        elif args.mode == "failure":
            failure(sc, args.input)
        else:
            reads(sc, args.input, use_cache=args.mode == "cache",
                  hold_seconds=args.hold_seconds)
    finally:
        sc.stop()
