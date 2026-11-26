"""
Soal 3 - Spark DataFrame dan SQL.
Jalankan: python soal3_sql.py
Variabel opsional: SNAPSHOT_PATH, REPEAT (3), OPT_PARTITIONS (8), K (paksa nilai k), K_MIN (3),
                   DO_BROADCAST (1 = jalankan juga demo broadcast join), KEEP_UI (1)
Hasil: ringkasan di terminal + CSV di folder out/ (dipakai grafik_analisis.py)
"""
import csv
import json
import math
import os
import time

from pyspark.sql import functions as F

from config import OUT_DIR, SNAPSHOT_PATH, edit_schema, make_spark, spark_path, wait_for_ui

REPEAT = int(os.environ.get("REPEAT", "3"))
OPT_PARTITIONS = int(os.environ.get("OPT_PARTITIONS", "8"))
K_MIN = int(os.environ.get("K_MIN", "3"))

# Tabel referensi wiki -> bahasa (untuk demo broadcast join)
REF = [
    ("idwiki", "Indonesia"), ("enwiki", "Inggris"), ("wikidatawiki", "Data terstruktur (Wikidata)"),
    ("commonswiki", "Multibahasa (Commons)"), ("metawiki", "Multibahasa (Meta)"), ("dewiki", "Jerman"),
    ("frwiki", "Prancis"), ("eswiki", "Spanyol"), ("ruwiki", "Rusia"), ("jawiki", "Jepang"),
    ("zhwiki", "Mandarin"), ("itwiki", "Italia"), ("ptwiki", "Portugis"), ("arwiki", "Arab"),
    ("plwiki", "Polandia"), ("ukwiki", "Ukraina"), ("fawiki", "Persia"), ("viwiki", "Vietnam"),
    ("kowiki", "Korea"), ("trwiki", "Turki"), ("svwiki", "Swedia"), ("jvwiki", "Jawa"),
    ("suwiki", "Sunda"), ("simplewiki", "Inggris sederhana"),
]


def save_csv(sdf, name):
    rows = sdf.collect()
    path = os.path.join(OUT_DIR, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(sdf.columns)
        for r in rows:
            w.writerow([("" if v is None else v) for v in r])
    print("   (disimpan: %s, %d baris)" % (path, len(rows)))
    return rows


def bench(spark, make_df, label, repeat):
    """Jalankan agregasi berat (write noop) beberapa kali, kembalikan daftar waktu."""
    sc = spark.sparkContext
    times = []
    for i in range(repeat):
        sc.setJobGroup("3d_%s_%d" % (label, i + 1), "3d %s run %d" % (label, i + 1))
        t0 = time.time()
        make_df().write.format("noop").mode("overwrite").save()
        times.append(time.time() - t0)
    return times


def main():
    spark = make_spark("UTS-Soal3-SQL")
    sc = spark.sparkContext
    print("Spark %s | master=%s" % (spark.version, sc.master))

    # ---------- 3a ----------
    print("\n===== 3a. DataFrame skema eksplisit + temporary view =====")
    raw = spark.read.schema(edit_schema()).json(spark_path(SNAPSHOT_PATH))
    df = (raw.filter(F.col("wiki").isNotNull())
          .withColumn("timestamp", F.col("timestamp").cast("timestamp"))
          .withColumn("bot", F.coalesce(F.col("bot"), F.lit(False)))
          .withColumn("selisih_byte", F.col("length.new") - F.coalesce(F.col("length.old"), F.lit(0))))
    df.cache()
    n = df.count()
    print("Jumlah baris valid: %d" % n)
    df.printSchema()
    df.select("id", "type", "wiki", "title", "user", "bot", "timestamp", "length", "selisih_byte").show(5, truncate=False)
    df.createOrReplaceTempView("edits")

    # ---------- 3b ----------
    print("\n===== 3b. Kueri SQL =====")
    print("(i) Jumlah edit per menit pada 5 wiki teratas (type=edit):")
    q1 = """
        WITH top5 AS (
            SELECT wiki FROM edits WHERE type = 'edit' GROUP BY wiki ORDER BY COUNT(*) DESC LIMIT 5)
        SELECT wiki, date_trunc('minute', timestamp) AS menit, COUNT(*) AS jumlah_edit
        FROM edits
        WHERE type = 'edit' AND wiki IN (SELECT wiki FROM top5)
        GROUP BY wiki, date_trunc('minute', timestamp)
        ORDER BY menit, wiki"""
    r1 = spark.sql(q1)
    r1.show(15, truncate=False)
    save_csv(r1, "s3_edits_per_menit.csv")

    print("(ii) Persentase bot dan manusia per wiki:")
    q2 = """
        SELECT wiki, COUNT(*) AS total,
               SUM(CASE WHEN bot THEN 1 ELSE 0 END) AS bot,
               SUM(CASE WHEN bot THEN 0 ELSE 1 END) AS manusia,
               ROUND(100.0 * SUM(CASE WHEN bot THEN 1 ELSE 0 END) / COUNT(*), 2) AS persen_bot,
               ROUND(100.0 * SUM(CASE WHEN bot THEN 0 ELSE 1 END) / COUNT(*), 2) AS persen_manusia
        FROM edits GROUP BY wiki ORDER BY total DESC"""
    r2 = spark.sql(q2)
    r2.show(15, truncate=False)
    rows2 = save_csv(r2, "s3_bot_persen.csv")

    print("   Uji konsistensi dengan hasil RDD (Soal 2):")
    p = os.path.join(OUT_DIR, "s2_hasil.json")
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            rdd = json.load(f)["bot_manusia_per_wiki"]
        beda = 0
        for r in rows2:
            d = rdd.get(r["wiki"])
            if d is None or d["bot"] != r["bot"] or d["manusia"] != r["manusia"]:
                beda += 1
        extra = len(set(rdd) - set(r["wiki"] for r in rows2))
        print("   Wiki dibandingkan: %d | berbeda: %d | wiki hanya di RDD: %d -> %s"
              % (len(rows2), beda, extra, "KONSISTEN" if beda == 0 and extra == 0 else "TIDAK KONSISTEN (cek filter/snapshot)"))
    else:
        print("   out/s2_hasil.json belum ada. Jalankan soal2_rdd.py dulu (dengan snapshot yang sama).")

    print("(iii) Rata-rata dan median abs(selisih_byte) per type:")
    r3 = spark.sql("""
        SELECT type, COUNT(*) AS n,
               ROUND(AVG(ABS(selisih_byte)), 2) AS rata2_abs_selisih,
               percentile_approx(ABS(selisih_byte), 0.5) AS median_abs_selisih
        FROM edits GROUP BY type ORDER BY n DESC""")
    r3.show(truncate=False)
    save_csv(r3, "s3_selisih_per_type.csv")

    print("(iv) 10 halaman idwiki dengan editor unik terbanyak (type=edit):")
    r4 = spark.sql("""
        SELECT title, COUNT(DISTINCT user) AS editor_unik, COUNT(*) AS jumlah_edit
        FROM edits WHERE wiki = 'idwiki' AND type = 'edit'
        GROUP BY title ORDER BY editor_unik DESC, jumlah_edit DESC LIMIT 10""")
    r4.show(truncate=False)
    save_csv(r4, "s3_idwiki_editor_unik.csv")

    # ---------- 3c ----------
    print("\n===== 3c. Deteksi lonjakan (jendela 5 menit) =====")
    win = (df.filter(F.col("type") == "edit")
           .groupBy(F.window("timestamp", "5 minutes").alias("w"), "wiki", "title")
           .agg(F.count("*").alias("n_edit")))
    win.cache()
    total_win = win.count()
    qs = win.select(F.percentile_approx("n_edit", [0.5, 0.9, 0.95, 0.99, 0.999]).alias("q")).first()["q"]
    mx = win.agg(F.max("n_edit")).first()[0]
    print("Jumlah kombinasi (halaman, jendela 5 menit): %d" % total_win)
    print("Distribusi n_edit: median=%s p90=%s p95=%s p99=%s p99.9=%s maks=%s" % (qs[0], qs[1], qs[2], qs[3], qs[4], mx))
    k_env = os.environ.get("K")
    k = int(k_env) if k_env else max(int(math.ceil(qs[3])), K_MIN)
    print("Ambang k = %d  (persentil ke-99 = %s; batas bawah K_MIN = %d)" % (k, qs[3], K_MIN))
    surge = (win.filter(F.col("n_edit") >= k).orderBy(F.desc("n_edit"))
             .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                     "wiki", "title", "n_edit"))
    print("Halaman dengan >= %d edit dalam 5 menit (jumlah: %d), teratas:" % (k, surge.count()))
    surge.show(15, truncate=False)
    save_csv(surge.limit(500), "s3_lonjakan.csv")
    win.unpersist()

    # ---------- 3d ----------
    print("\n===== 3d. explain() dan optimasi =====")
    spark.conf.set("spark.sql.adaptive.enabled", "false")  # agar plan klasik (Exchange + HashAggregate) terlihat jelas
    agg = df.groupBy("wiki").agg(F.count("*").alias("n"))
    print("Physical plan agregasi groupBy(wiki).count():")
    agg.explain()
    print("Baca: HashAggregate (partial_count) -> Exchange hashpartitioning(wiki) -> HashAggregate (final).")
    print("Exchange = batas Stage (shuffle). Cocokkan dengan Spark UI > tab SQL dan tab Stages.\n")

    def heavy():
        return df.groupBy("wiki", "title").agg(F.count("*").alias("n"), F.countDistinct("user").alias("u"))

    print("Optimasi: spark.sql.shuffle.partitions 200 (bawaan) vs %d" % OPT_PARTITIONS)
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    t_base = bench(spark, heavy, "p200", REPEAT)
    spark.conf.set("spark.sql.shuffle.partitions", str(OPT_PARTITIONS))
    t_opt = bench(spark, heavy, "p%d" % OPT_PARTITIONS, REPEAT)
    a, b = sum(t_base) / len(t_base), sum(t_opt) / len(t_opt)
    print("  200 partisi : %s | rata-rata %.3f detik" % (["%.2f" % t for t in t_base], a))
    print("  %3d partisi : %s | rata-rata %.3f detik" % (OPT_PARTITIONS, ["%.2f" % t for t in t_opt], b))
    print("  Perubahan   : %.1f%% (%s)" % (100.0 * (a - b) / a, "lebih cepat" if b < a else "tidak lebih cepat"))
    print("  -> Spark UI > Stages: bandingkan jumlah task stage shuffle (200 vs %d). Screenshot." % OPT_PARTITIONS)

    if os.environ.get("DO_BROADCAST", "0") == "1":
        print("\nDemo broadcast join (tabel referensi wiki -> bahasa):")
        ref = spark.createDataFrame(REF, ["wiki", "bahasa"])
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        j1 = df.join(ref, "wiki", "left").groupBy(F.coalesce("bahasa", F.lit("Lainnya")).alias("bahasa")).count()
        print("Tanpa broadcast (SortMergeJoin):")
        j1.explain()
        t1 = bench(spark, lambda: j1, "join_smj", REPEAT)
        j2 = df.join(F.broadcast(ref), "wiki", "left").groupBy(F.coalesce("bahasa", F.lit("Lainnya")).alias("bahasa")).count()
        print("Dengan broadcast (BroadcastHashJoin):")
        j2.explain()
        t2 = bench(spark, lambda: j2, "join_bhj", REPEAT)
        print("  SortMergeJoin rata-rata %.3f detik | BroadcastHashJoin rata-rata %.3f detik"
              % (sum(t1) / len(t1), sum(t2) / len(t2)))

    wait_for_ui()
    spark.stop()


if __name__ == "__main__":
    main()
