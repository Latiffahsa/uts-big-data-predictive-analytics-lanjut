"""
Soal 1c - Consumer untuk uji decoupling.

Langkah uji (producer tetap berjalan di terminal lain):
  1. python soal1c_consumer.py          (sesi 1, biarkan beberapa menit, lalu Ctrl+C)
  2. tunggu sekitar 2 menit (producer jalan terus)
  3. python soal1c_consumer.py          (sesi 2, group SAMA -> lanjut dari offset terakhir)
  4. Ctrl+C, lalu: python soal1c_analisis.py

Offset di-commit MANUAL setelah pesan dicatat -> garansi at-least-once.
"""
import json
import os
import time

from kafka import KafkaConsumer

from config import BOOTSTRAP, OUT_DIR, TOPIC

GROUP = os.environ.get("CONSUMER_GROUP", "cg-decoupling")
RESET = os.environ.get("RESET", "latest")  # dipakai hanya jika group belum punya committed offset
LOG = os.path.join(OUT_DIR, "consumer_log.jsonl")
COMMIT_EVERY = 200
STOP_FILE = os.environ.get("STOP_FILE", "STOP_consumer.flag")


def committed_offset(consumer, tp):
    try:
        c = consumer.committed(tp)
    except Exception:
        return None
    return getattr(c, "offset", c)


def safe_commit(consumer):
    try:
        consumer.commit()
    except Exception as e:
        print("Gagal commit offset:", repr(e))


def main():
    sid = time.strftime("%Y%m%d-%H%M%S")
    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP,
        enable_auto_commit=False,
        auto_offset_reset=RESET,
        max_poll_records=500,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    print("Sesi %s | group=%s | topic=%s (Ctrl+C untuk berhenti)" % (sid, GROUP, TOPIC))
    per = {}
    total = 0
    since_commit = 0
    announced = False
    last_print = time.time()
    f = open(LOG, "a", encoding="utf-8")
    f.write(json.dumps({"session": sid, "event": "start", "group": GROUP}) + "\n")
    try:
        while not os.path.exists(STOP_FILE):
            batch = consumer.poll(timeout_ms=1000)
            if not announced and consumer.assignment():
                announced = True
                for tp in sorted(consumer.assignment(), key=lambda t: t.partition):
                    co = committed_offset(consumer, tp)
                    print("Partisi %d: committed offset sebelum sesi ini = %s" % (tp.partition, co))
                    f.write(json.dumps({"session": sid, "event": "committed", "p": tp.partition, "committed": co}) + "\n")
            for tp, recs in batch.items():
                for r in recs:
                    f.write(json.dumps({"session": sid, "p": r.partition, "o": r.offset, "k": r.key, "ts": r.timestamp}) + "\n")
                    st = per.setdefault(r.partition, [r.offset, r.offset, 0])
                    st[1] = r.offset
                    st[2] += 1
                    total += 1
                    since_commit += 1
            if batch:
                f.flush()
            if since_commit >= COMMIT_EVERY:
                safe_commit(consumer)
                since_commit = 0
            if time.time() - last_print >= 10:
                print("[%s] diterima=%d  %s" % (time.strftime("%H:%M:%S"), total,
                      {p: "o=%d..%d" % (v[0], v[1]) for p, v in sorted(per.items())}))
                last_print = time.time()
    except KeyboardInterrupt:
        print("\nDihentikan.")
    finally:
        f.flush()
        safe_commit(consumer)
        print("Ringkasan sesi %s (total %d pesan):" % (sid, total))
        for p, (first, last, n) in sorted(per.items()):
            print("  partisi %d: offset pertama=%d  terakhir=%d  jumlah=%d" % (p, first, last, n))
        f.write(json.dumps({"session": sid, "event": "stop", "total": total}) + "\n")
        f.close()
        consumer.close()


if __name__ == "__main__":
    main()
