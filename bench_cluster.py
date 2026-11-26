"""
Benchmark standalone vs terdistribusi (Bagian 4.5 laporan).
Pekerjaan sama (Soal 2b(i): edit manusia per wiki, reduceByKey), dijalankan dengan master berbeda.

  python bench_cluster.py --master "local[1]"  --label standalone-1core
  python bench_cluster.py --master "spark://IP_MASTER:7077" --label cluster --cores-max 4
Hasil ditambahkan ke out/bench_cluster.jsonl
"""
import argparse
import json
import os
import time
from operator import add

from pyspark.sql import SparkSession

from config import OUT_DIR, SNAPSHOT_PATH, spark_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--cores-max", type=int, default=0)
    a = ap.parse_args()
    b = SparkSession.builder.appName("UTS-Bench-%s" % a.label).master(a.master)
    if os.environ.get("DRIVER_HOST") and a.master.startswith("spark://"):
        b = b.config("spark.driver.host", os.environ["DRIVER_HOST"]).config("spark.driver.bindAddress", "0.0.0.0")
    if a.cores_max:
        b = b.config("spark.cores.max", str(a.cores_max))
    spark = b.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    def parse(line):
        try:
            import json as _j
            e = _j.loads(line)
            return [e] if isinstance(e, dict) else []
        except ValueError:
            return []

    def job():
        lines = sc.textFile(spark_path(SNAPSHOT_PATH), minPartitions=a.partitions)
        return (lines.flatMap(parse)
                .filter(lambda e: e.get("type") == "edit" and e.get("bot") is False)
                .map(lambda e: (e.get("wiki"), 1)).reduceByKey(add)
                .takeOrdered(10, key=lambda kv: (-kv[1], str(kv[0]))))

    times = []
    for i in range(a.repeat):
        t0 = time.time()
        res = job()
        times.append(time.time() - t0)
        print("[%s] run %d: %.2f detik" % (a.label, i + 1, times[-1]))
    row = {"label": a.label, "master": a.master, "parallelism": sc.defaultParallelism,
           "runs": [round(t, 3) for t in times], "avg_all": round(sum(times) / len(times), 3),
           "avg_warm": round(sum(times[1:]) / max(len(times) - 1, 1), 3), "top1": res[0] if res else None}
    with open(os.path.join(OUT_DIR, "bench_cluster.jsonl"), "a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")
    print("Hasil: %s" % row)
    from config import wait_for_ui
    wait_for_ui()
    spark.stop()


if __name__ == "__main__":
    main()
