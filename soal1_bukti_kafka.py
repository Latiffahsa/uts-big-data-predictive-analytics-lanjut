"""
Soal 1b - Bukti dari Kafka: jumlah pesan, offset tiap partisi, laju (pesan/detik).
Pelengkap keluaran perintah CLI Kafka (kafka-topics, kafka-get-offsets).

Jalankan:
  python soal1_bukti_kafka.py          -> jumlah pesan, offset, laju rata-rata dari timestamp pesan
  python soal1_bukti_kafka.py 60       -> ditambah pengukuran laju langsung selama 60 detik
                                          (producer harus sedang berjalan)
"""
import sys
import time

from kafka import KafkaConsumer, TopicPartition

from config import BOOTSTRAP, TOPIC


def get_partitions(consumer):
    parts = None
    for _ in range(10):
        parts = consumer.partitions_for_topic(TOPIC)
        if parts:
            break
        time.sleep(1)
    return [TopicPartition(TOPIC, p) for p in sorted(parts or [])]


def read_ts(consumer, tp, offset):
    consumer.seek(tp, offset)
    for _ in range(5):
        recs = consumer.poll(timeout_ms=1000, max_records=1)
        for r in recs.get(tp, []):
            return r.timestamp
    return None


def main():
    sample = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=None, enable_auto_commit=False)
    tps = get_partitions(consumer)
    if not tps:
        raise SystemExit("Topic %s tidak ditemukan. Cek Kafka dan nama topic." % TOPIC)
    consumer.assign(tps)
    beg = consumer.beginning_offsets(tps)
    end = consumer.end_offsets(tps)

    print("Topic: %s | jumlah partisi: %d" % (TOPIC, len(tps)))
    print("%-10s %14s %14s %12s" % ("Partisi", "Offset awal", "Offset akhir", "Jumlah"))
    total = 0
    tmin = None
    tmax = None
    for tp in tps:
        n = end[tp] - beg[tp]
        total += n
        print("%-10d %14d %14d %12d" % (tp.partition, beg[tp], end[tp], n))
        if n > 0:
            a = read_ts(consumer, tp, beg[tp])
            b = read_ts(consumer, tp, end[tp] - 1)
            if a is not None:
                tmin = a if tmin is None else min(tmin, a)
            if b is not None:
                tmax = b if tmax is None else max(tmax, b)
    print("TOTAL pesan di topic: %d" % total)
    if tmin is not None and tmax is not None and tmax > tmin:
        durasi = (tmax - tmin) / 1000.0
        print("Pesan pertama : %s" % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tmin / 1000)))
        print("Pesan terakhir: %s" % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tmax / 1000)))
        print("Rentang waktu : %.0f detik (%.1f menit)" % (durasi, durasi / 60))
        print("Laju rata-rata: %.2f pesan/detik (total / rentang waktu)" % (total / durasi))
        print("Catatan: bila ada jeda (mis. laptop tidur), laju rata-rata ini lebih rendah dari laju sebenarnya.")

    if sample > 0:
        e0 = consumer.end_offsets(tps)
        t0 = time.time()
        print("\nMengukur laju langsung selama %d detik..." % sample)
        time.sleep(sample)
        e1 = consumer.end_offsets(tps)
        dt = time.time() - t0
        delta = sum(e1[tp] - e0[tp] for tp in tps)
        print("Pertambahan pesan: %d dalam %.1f detik -> laju %.2f pesan/detik" % (delta, dt, delta / dt))
    consumer.close()


if __name__ == "__main__":
    main()
