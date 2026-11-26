"""
Ekspor snapshot topic Kafka ke berkas JSONL (dibaca Spark pada Soal 2, 3).
Membaca dari offset awal sampai offset akhir PADA SAAT skrip dimulai (snapshot).

Jalankan: python export_jsonl.py
Hasil   : SNAPSHOT_PATH (bawaan: data/wiki_snapshot.jsonl)
"""
import os
import time

from kafka import KafkaConsumer, TopicPartition

from config import BOOTSTRAP, SNAPSHOT_PATH, TOPIC


def main():
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=None, enable_auto_commit=False)
    parts = None
    for _ in range(10):
        parts = consumer.partitions_for_topic(TOPIC)
        if parts:
            break
        time.sleep(1)
    if not parts:
        raise SystemExit("Topic %s tidak ditemukan." % TOPIC)
    tps = [TopicPartition(TOPIC, p) for p in sorted(parts)]
    consumer.assign(tps)
    beg = consumer.beginning_offsets(tps)
    end = consumer.end_offsets(tps)
    for tp in tps:
        consumer.seek(tp, beg[tp])
    target = sum(end[tp] - beg[tp] for tp in tps)
    print("Mengekspor %d pesan dari %d partisi ke %s" % (target, len(tps), SNAPSHOT_PATH))
    n = 0
    t0 = time.time()
    with open(SNAPSHOT_PATH, "w", encoding="utf-8", newline="\n") as f:
        while any(consumer.position(tp) < end[tp] for tp in tps):
            batch = consumer.poll(timeout_ms=1000, max_records=5000)
            for tp, recs in batch.items():
                for r in recs:
                    if r.offset >= end[tp]:
                        continue
                    f.write(r.value.decode("utf-8").replace("\n", " ") + "\n")
                    n += 1
            if n and n % 50000 < 5000:
                print("  ...%d / %d" % (n, target))
    consumer.close()
    size_mb = os.path.getsize(SNAPSHOT_PATH) / 1e6
    print("Selesai: %d baris, %.1f MB, %.0f detik" % (n, size_mb, time.time() - t0))


if __name__ == "__main__":
    main()
