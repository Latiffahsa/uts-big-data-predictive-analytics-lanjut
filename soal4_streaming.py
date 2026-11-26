"""
Soal 4 - Spark Structured Streaming dari Kafka.

Contoh pemakaian (producer harus berjalan di terminal lain):
  4a + 4b trigger 10 detik : python soal4_streaming.py --trigger 10 --duration 180 --tag t10
  4b trigger 30 detik      : python soal4_streaming.py --trigger 30 --duration 180 --tag t30
  4c sesi 1                : python soal4_streaming.py --trigger 10 --duration 120 --tag run1 --checkpoint out/chk/uji4c
  4c sesi 2 (checkpoint sama, tag beda): python soal4_streaming.py --trigger 10 --duration 120 --tag run2 --checkpoint out/chk/uji4c
  lalu: python soal4c_verifikasi.py run1 run2

Opsi lain: --starting latest|earliest (hanya untuk run pertama pada checkpoint baru),
           --max-offsets N (batasi pesan per batch), --only-edit (hitung type=edit saja)
"""
import argparse
import json
import os
import time

from pyspark.sql import functions as F

from config import BOOTSTRAP, OUT_DIR, TOPIC, edit_schema, make_spark, spark_path


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger", type=int, default=10, help="interval trigger (detik)")
    ap.add_argument("--duration", type=int, default=180, help="lama query berjalan (detik)")
    ap.add_argument("--tag", default="run1", help="nama untuk berkas log progress")
    ap.add_argument("--checkpoint", default=None, help="lokasi checkpoint")
    ap.add_argument("--starting", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--max-offsets", type=int, default=0)
    ap.add_argument("--only-edit", action="store_true")
    return ap.parse_args()


def main():
    args = parse_args()
    ckpt = args.checkpoint or os.path.join(OUT_DIR, "chk", "trigger%d" % args.trigger)
    ckpt = os.path.abspath(ckpt)
    out_dir = os.path.abspath(os.path.join(OUT_DIR, "stream_batches"))
    spark = make_spark("UTS-Soal4-Streaming", kafka=True, shuffle_partitions=8)
    print("Spark %s | master=%s | checkpoint=%s | trigger=%ds | tag=%s"
          % (spark.version, spark.sparkContext.master, ckpt, args.trigger, args.tag))

    # ---- 4a: readStream -> from_json -> event time -> window 1 menit + watermark 2 menit
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", TOPIC)
              .option("startingOffsets", args.starting)
              .option("failOnDataLoss", "false"))
    if args.max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", args.max_offsets)
    raw = reader.load()
    ev = (raw.select(F.from_json(F.col("value").cast("string"), edit_schema()).alias("e")).select("e.*")
          .filter(F.col("wiki").isNotNull())
          .withColumn("event_time", F.col("timestamp").cast("timestamp")))
    if args.only_edit:
        ev = ev.filter(F.col("type") == "edit")
    agg = (ev.withWatermark("event_time", "2 minutes")
           .groupBy(F.window("event_time", "1 minute"), "wiki")
           .agg(F.count("*").alias("n_edit"),
                F.round(F.avg(F.coalesce(F.col("bot"), F.lit(False)).cast("int")), 4).alias("rasio_bot")))

    def save_batch(batch_df, batch_id):
        batch_df.persist()
        flat = (batch_df.select(F.col("window.start").alias("window_start"),
                                F.col("window.end").alias("window_end"), "wiki", "n_edit", "rasio_bot")
                .withColumn("batch_id", F.lit(int(batch_id))).withColumn("tag", F.lit(args.tag)))
        n = flat.count()
        print("[batch %d] baris hasil (jendela x wiki) = %d" % (batch_id, n))
        flat.orderBy(F.desc("n_edit")).show(5, truncate=False)      # keluaran ke console
        flat.coalesce(1).write.mode("append").option("header", True).csv(spark_path(out_dir))  # simpan tiap batch
        batch_df.unpersist()

    query = (agg.writeStream.outputMode("update")
             .foreachBatch(save_batch)
             .option("checkpointLocation", spark_path(ckpt))
             .trigger(processingTime="%d seconds" % args.trigger)
             .queryName("wiki_%s" % args.tag).start())

    # ---- 4b: catat progress (lastProgress / recentProgress)
    log_path = os.path.join(OUT_DIR, "progress_%s.jsonl" % args.tag)
    seen = set()
    stats = []

    def sweep(f):
        for p in list(query.recentProgress):
            key = p.get("timestamp")
            if key in seen:
                continue
            seen.add(key)
            f.write(json.dumps(p, default=str) + "\n")
            f.flush()
            if p.get("numInputRows", 0) > 0:
                stats.append(p)
                print("[progress] batch=%s input=%s baris | inputRowsPerSecond=%s | processedRowsPerSecond=%s | triggerExecution=%s ms"
                      % (p.get("batchId"), p.get("numInputRows"), p.get("inputRowsPerSecond"),
                         p.get("processedRowsPerSecond"), (p.get("durationMs") or {}).get("triggerExecution")))

    t_end = time.time() + args.duration
    with open(log_path, "a", encoding="utf-8") as f:
        while time.time() < t_end and query.isActive:
            time.sleep(2)
            sweep(f)
        sweep(f)
        query.stop()
    print("\nQuery dihentikan. Log progress: %s" % log_path)
    if query.exception():
        print("Query berhenti karena error:", query.exception())
    if stats:
        def avg(fn):
            vals = [fn(p) for p in stats if fn(p) is not None]
            return sum(vals) / len(vals) if vals else float("nan")
        print("RINGKASAN trigger %ds (%d batch berisi data):" % (args.trigger, len(stats)))
        print("  rata-rata inputRowsPerSecond     : %.1f" % avg(lambda p: p.get("inputRowsPerSecond")))
        print("  rata-rata processedRowsPerSecond : %.1f" % avg(lambda p: p.get("processedRowsPerSecond")))
        print("  rata-rata durasi batch (ms)      : %.0f" % avg(lambda p: (p.get("durationMs") or {}).get("triggerExecution")))
        print("  rata-rata baris per batch        : %.0f" % avg(lambda p: p.get("numInputRows")))
    else:
        print("Belum ada batch berisi data (apakah producer berjalan?).")
    spark.stop()


if __name__ == "__main__":
    main()
