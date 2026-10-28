"""Part 1: one RDD chain, one final action, warmup plus three repeats."""

import argparse
import json
import statistics
import time

from pyspark import SparkConf, SparkContext

from pipeline import DEFAULT_INPUT, build_pipeline, merge_leaders, money


def run(app_name="SparkLab-Part1", input_path=DEFAULT_INPUT):
    sc = SparkContext(conf=SparkConf().setAppName(app_name))
    sc.setLogLevel("WARN")
    try:
        times = []
        expected = None
        for index in range(4):
            # A fresh textFile and full lineage for every timed action.
            build_start = time.perf_counter()
            lines, leader_rdd = build_pipeline(sc, input_path)
            build_seconds = time.perf_counter() - build_start
            if index == 0:
                print(f"INPUT={input_path}", flush=True)
                print(f"SOURCE_PARTITIONS={lines.getNumPartitions()}", flush=True)
                print("LEADER_LINEAGE_BEGIN", flush=True)
                print(leader_rdd.toDebugString().decode("utf-8"), flush=True)
                print("LEADER_LINEAGE_END", flush=True)
            label = "warmup" if index == 0 else f"repeat-{index}"
            sc.setJobGroup(label, f"{app_name} {label} {input_path}")
            action_start = time.perf_counter()
            result = leader_rdd.reduce(merge_leaders)  # The only action in this run.
            action_seconds = time.perf_counter() - action_start
            if expected is not None and result != expected:
                raise AssertionError("The repeated RDD result changed")
            expected = result
            if index:
                times.append(action_seconds)
            print(json.dumps({
                "run": label, "build_s": round(build_seconds, 6),
                "action_s": round(action_seconds, 6),
                "leader": result[0], "leader_cents": result[1],
                "task1_records": result[2],
            }), flush=True)
        print(f"RESULT leader={expected[0]} amount={money(expected[1])} "
              f"task1_records={expected[2]}", flush=True)
        print(f"ACTION_MEDIAN_S={statistics.median(times):.6f}", flush=True)
    finally:
        sc.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--app-name", default="SparkLab-Part1")
    args = parser.parse_args()
    run(args.app_name, args.input)
