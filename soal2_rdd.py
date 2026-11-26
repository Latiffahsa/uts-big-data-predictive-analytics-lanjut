"""
Soal 2 - Spark RDD (hanya operasi RDD).
Jalankan: python soal2_rdd.py
Variabel opsional: SNAPSHOT_PATH, MIN_PARTITIONS (8), REPEAT (3), SPARK_MASTER, KEEP_UI (1)
Hasil: ringkasan di terminal + out/s2_hasil.json (dipakai Soal 3 untuk uji konsistensi)
"""
import json
import os
import re
import time
from operator import add

from config import OUT_DIR, SNAPSHOT_PATH, make_spark, spark_path, wait_for_ui

MIN_PARTITIONS = int(os.environ.get("MIN_PARTITIONS", "8"))
REPEAT = int(os.environ.get("REPEAT", "3"))

STOPWORDS = set("""
yang dan di ke dari dengan untuk pada ini itu atau juga dalam adalah oleh sebagai tidak akan dapat
telah sudah ada lebih serta karena agar bahwa saya kami kita mereka anda nya lah kah pun para antara
hingga hanya sebuah suatu setelah sebelum seperti saat setiap dll dsb yg dg utk dlm pd sdh tdk
the of and to in is it for on as with by at from an be this that are was or not has have had but
""".split())
TOKEN = re.compile(r"[a-zA-Z]{3,}")


def make_parser(bad):
    """Parse satu baris JSON. Baris rusak DIHITUNG dengan accumulator (tidak dibuang diam-diam)."""
    def parse(line):
        try:
            ev = json.loads(line)
            if not isinstance(ev, dict):
                raise ValueError("bukan objek JSON")
            return [ev]
        except ValueError:
            bad.add(1)
            return []
    return parse


def is_human_edit(e):
    return e.get("type") == "edit" and e.get("bot") is False


def is_idwiki_edit(e):
    return e.get("wiki") == "idwiki" and e.get("type") == "edit"


def comment_words(e):
    c = e.get("comment") or ""
    c = re.sub(r"/\*.*?\*/", " ", c)  # buang penanda bagian /* ... */
    return [w for w in (t.lower() for t in TOKEN.findall(c)) if w not in STOPWORDS]


def sort_key(kv):
    return (-kv[1], str(kv[0]))


def timed(sc, group, desc, fn):
    sc.setJobGroup(group, desc)
    t0 = time.time()
    res = fn()
    return res, time.time() - t0


def job_stats(sc, group):
    tr = sc.statusTracker()
    jobs = tr.getJobIdsForGroup(group)
    stages, tasks = set(), 0
    for j in jobs:
        info = tr.getJobInfo(j)
        if not info:
            continue
        for s in info.stageIds:
            if s in stages:
                continue
            stages.add(s)
            si = tr.getStageInfo(s)
            if si:
                tasks += si.numTasks
    return len(jobs), len(stages), tasks


def lineage(rdd):
    s = rdd.toDebugString()
    return s.decode("utf-8") if isinstance(s, bytes) else s


def main():
    spark = make_spark("UTS-Soal2-RDD")
    sc = spark.sparkContext
    print("Spark %s | master=%s | snapshot=%s" % (spark.version, sc.master, SNAPSHOT_PATH))
    groups = []  # (label, group) untuk tabel Job-Stage-Task

    # ---------- 2a ----------
    print("\n===== 2a. Parsing, data rusak, partisi =====")
    bad = sc.accumulator(0)
    lines = sc.textFile(spark_path(SNAPSHOT_PATH), minPartitions=MIN_PARTITIONS)
    parsed = lines.flatMap(make_parser(bad))
    n, dt = timed(sc, "2a_count", "2a parse + count", parsed.count)
    groups.append(("2a count() pada RDD hasil parsing", "2a_count"))
    bad_after = bad.value  # dibaca tepat setelah action pertama
    print("Baris valid           : %d (%.2f detik)" % (n, dt))
    print("Baris rusak (accumul.): %d" % bad_after)
    print("Jumlah partisi RDD    : %d" % parsed.getNumPartitions())
    sizes, _ = timed(sc, "2a_glom", "2a glom", lambda: parsed.glom().map(len).collect())
    groups.append(("2a glom().map(len).collect()", "2a_glom"))
    print("Elemen per partisi    : %s" % sizes)
    print("  min=%d  max=%d  rata-rata=%.0f" % (min(sizes), max(sizes), sum(sizes) / len(sizes)))

    # ---------- 2b ----------
    print("\n===== 2b. Operasi RDD =====")
    pairs = parsed.filter(is_human_edit).map(lambda e: (e.get("wiki"), 1))
    top_wiki, _ = timed(sc, "2b_i", "2b(i) top wiki edit manusia",
                        lambda: pairs.reduceByKey(add).takeOrdered(10, key=sort_key))
    groups.append(("2b(i) 10 wiki edit manusia terbanyak", "2b_i"))
    print("(i) 10 wiki dengan edit manusia (type=edit, bot=false) terbanyak:")
    for w, c in top_wiki:
        print("    %-18s %d" % (w, c))

    top_pages, _ = timed(sc, "2b_ii", "2b(ii) top halaman idwiki",
                         lambda: parsed.filter(is_idwiki_edit).map(lambda e: (e.get("title"), 1))
                         .reduceByKey(add).takeOrdered(10, key=sort_key))
    groups.append(("2b(ii) 10 halaman idwiki terbanyak", "2b_ii"))
    print("(ii) 10 halaman paling banyak disunting di idwiki (type=edit):")
    for t, c in top_pages:
        print("    %-45s %d" % (t, c))

    top_words, _ = timed(sc, "2b_iii", "2b(iii) top kata comment idwiki",
                         lambda: parsed.filter(is_idwiki_edit).flatMap(comment_words)
                         .map(lambda w: (w, 1)).reduceByKey(add).takeOrdered(15, key=sort_key))
    groups.append(("2b(iii) 15 kata terbanyak comment idwiki", "2b_iii"))
    print("(iii) 15 kata terbanyak pada comment idwiki (stopword dibuang):")
    for w, c in top_words:
        print("    %-20s %d" % (w, c))

    print("\nPenandaan narrow/wide:")
    for op, kind, ket in [
        ("textFile", "-", "sumber data"),
        ("flatMap(parse)", "NARROW", "tiap partisi diproses sendiri"),
        ("filter", "NARROW", "tanpa shuffle"),
        ("map", "NARROW", "tanpa shuffle"),
        ("flatMap(comment_words)", "NARROW", "tanpa shuffle"),
        ("reduceByKey", "WIDE", "shuffle (gabung lokal dulu, lalu antar partisi)"),
        ("groupByKey", "WIDE", "shuffle semua pasangan"),
        ("takeOrdered", "ACTION", "memicu job, mengambil hasil ke driver"),
    ]:
        print("    %-26s %-7s %s" % (op, kind, ket))

    # data bot vs manusia per wiki (untuk uji konsistensi dengan SQL di Soal 3)
    bw, _ = timed(sc, "2x_botmanusia", "bot vs manusia per wiki",
                  lambda: parsed.map(lambda e: ((e.get("wiki"), bool(e.get("bot"))), 1))
                  .reduceByKey(add).collect())
    agg = {}
    for (w, is_bot), c in bw:
        d = agg.setdefault(w, {"bot": 0, "manusia": 0})
        d["bot" if is_bot else "manusia"] += c
    for w, d in agg.items():
        tot = d["bot"] + d["manusia"]
        d["total"] = tot
        d["persen_bot"] = round(100.0 * d["bot"] / tot, 2)
        d["persen_manusia"] = round(100.0 * d["manusia"] / tot, 2)

    # ---------- 2c ----------
    print("\n===== 2c. reduceByKey vs groupByKey =====")
    base = pairs.cache()
    base.count()  # pemanasan: isi cache agar selisih waktu murni dari shuffle
    funcs = [
        ("reduceByKey", lambda: base.reduceByKey(add).takeOrdered(10, key=sort_key)),
        ("groupByKey", lambda: base.groupByKey().mapValues(sum).takeOrdered(10, key=sort_key)),
    ]
    results, avg = {}, {}
    for name, fn in funcs:
        ts = []
        for i in range(REPEAT):
            g = "2c_%s_%d" % (name, i + 1)
            res, dt = timed(sc, g, "2c %s run %d" % (name, i + 1), fn)
            ts.append(dt)
            if i == 0:
                groups.append(("2c %s" % name, g))
        results[name] = res
        avg[name] = sum(ts) / len(ts)
        print("%-12s waktu per run: %s | rata-rata %.3f detik" % (name, ["%.3f" % t for t in ts], avg[name]))
    print("Hasil kedua metode sama: %s" % (results["reduceByKey"] == results["groupByKey"]))
    print("-> Buka Spark UI > Stages, cari job '2c reduceByKey' dan '2c groupByKey',")
    print("   catat Shuffle Write dan Shuffle Read masing-masing (screenshot).")
    base.unpersist()

    # ---------- 2d ----------
    print("\n===== 2d. Lineage, Job-Stage-Task, cache() =====")
    print("Lineage RDD hasil parsing (toDebugString):")
    print(lineage(parsed))
    print("\nLineage reduceByKey:")
    print(lineage(pairs.reduceByKey(add)))

    print("\nTabel Job-Stage-Task per action:")
    print("%-45s %6s %7s %6s" % ("Action", "Job", "Stage", "Task"))
    for label, g in groups:
        j, s, t = job_stats(sc, g)
        print("%-45s %6d %7d %6d" % (label, j, s, t))

    print("\nPengaruh cache() (RDD hasil parsing dipakai 3 action):")

    def three_actions(rdd, tag):
        out = {}
        _, out["count"] = timed(sc, tag + "_count", tag + " count", rdd.count)
        _, out["top_wiki"] = timed(sc, tag + "_wiki", tag + " top wiki",
                                   lambda: rdd.filter(is_human_edit).map(lambda e: (e.get("wiki"), 1))
                                   .reduceByKey(add).takeOrdered(10, key=sort_key))
        _, out["top_halaman"] = timed(sc, tag + "_page", tag + " top halaman",
                                      lambda: rdd.filter(is_idwiki_edit).map(lambda e: (e.get("title"), 1))
                                      .reduceByKey(add).takeOrdered(10, key=sort_key))
        return out

    p_nc = lines.flatMap(make_parser(sc.accumulator(0)))
    t_nc = three_actions(p_nc, "nocache")
    p_c = lines.flatMap(make_parser(sc.accumulator(0))).cache()
    _, t_mat = timed(sc, "cache_materialize", "cache: isi cache", p_c.count)
    t_c = three_actions(p_c, "cache")
    print("%-14s %14s %14s" % ("Action", "tanpa cache", "dengan cache"))
    for k in ("count", "top_wiki", "top_halaman"):
        print("%-14s %13.3fs %13.3fs" % (k, t_nc[k], t_c[k]))
    tot_nc, tot_c = sum(t_nc.values()), sum(t_c.values())
    print("%-14s %13.3fs %13.3fs  (+ %.3fs untuk mengisi cache sekali)" % ("TOTAL", tot_nc, tot_c, t_mat))
    print("-> Screenshot juga tab Storage di Spark UI (RDD ter-cache, ukuran, persen cached).")
    # p_c.unpersist()

    # simpan hasil
    out = {
        "n_valid": n, "n_rusak": bad_after, "partisi": parsed.getNumPartitions(), "elemen_per_partisi": sizes,
        "top_wiki_manusia": top_wiki, "top_halaman_idwiki": top_pages, "top_kata_idwiki": top_words,
        "bot_manusia_per_wiki": agg,
        "waktu_2c_rata2": avg, "waktu_cache": {"tanpa": t_nc, "dengan": t_c, "isi_cache": t_mat},
    }
    with open(os.path.join(OUT_DIR, "s2_hasil.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\nHasil disimpan di %s" % os.path.join(OUT_DIR, "s2_hasil.json"))
    wait_for_ui()
    spark.stop()


if __name__ == "__main__":
    main()
