"""
Producer UTS ST153 - Soal 1b
Wikimedia EventStreams (SSE) -> Kafka
- User-Agent deskriptif
- Reconnect otomatis (backoff) + Last-Event-ID
- Hanya field yang diperlukan dikirim
- user anonim (alamat IP) di-hash sebelum dikirim

Instal:  pip install requests kafka-python
Jalankan: python producer.py        (hentikan dengan Ctrl+C)
"""
import hashlib
import ipaddress
import json
import os
import time

import requests

# ====== PENGATURAN (ubah sesuai kebutuhan) ======
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "wiki.recentchange.raw")
PARTITIONS = 3
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# GANTI email di bawah dengan emailmu (Wikimedia meminta User-Agent yang jelas)
USER_AGENT = os.environ.get(
    "WIKI_UA", "UTS-ST153-WikiMonitor/1.0 (tugas kuliah AMIKOM; ISI_EMAIL_KAMU)"
)
# Salt rahasia untuk hash IP. Ganti, simpan sendiri, jangan dibagikan.
ANON_SALT = os.environ.get("ANON_SALT", "")
REPORT_EVERY_SEC = 30
STOP_FILE = os.environ.get("STOP_FILE", "STOP_producer.flag")  # buat berkas ini untuk berhenti dengan rapi
# =================================================


def is_ip(text):
    try:
        ipaddress.ip_address(text)
        return True
    except (ValueError, TypeError):
        return False


def anonymize(user):
    """User anonim di Wikipedia = alamat IP. Ganti dengan hash bergaram."""
    if user and is_ip(user):
        digest = hashlib.sha256((ANON_SALT + user).encode("utf-8")).hexdigest()
        return "anon_" + digest[:16]
    return user


def transform(ev):
    """Ambil hanya field yang diperlukan. Kembalikan None bila bukan event valid."""
    if not isinstance(ev, dict) or not ev.get("wiki"):
        return None
    length = ev.get("length") or {}
    return {
        "id": ev.get("id"),
        "type": ev.get("type"),
        "wiki": ev.get("wiki"),
        "server_name": ev.get("server_name"),
        "title": ev.get("title"),
        "namespace": ev.get("namespace"),
        "user": anonymize(ev.get("user")),
        "bot": ev.get("bot"),
        "minor": ev.get("minor"),
        "timestamp": ev.get("timestamp"),
        "length": {"old": length.get("old"), "new": length.get("new")},
        "comment": ev.get("comment"),
    }


def make_key(ev, msg):
    """Key pesan: wiki:id. Bila id kosong (mis. event log), pakai meta.id."""
    if msg["id"] is not None:
        return "%s:%s" % (msg["wiki"], msg["id"])
    meta_id = (ev.get("meta") or {}).get("id")
    return "%s:%s" % (msg["wiki"], meta_id) if meta_id else msg["wiki"]


def ensure_topic():
    from kafka.admin import KafkaAdminClient, NewTopic

    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        admin.create_topics(
            [NewTopic(name=TOPIC, num_partitions=PARTITIONS, replication_factor=1)]
        )
        print("Topic dibuat:", TOPIC)
    except Exception as e:  # topic sudah ada atau gagal dibuat
        print("Info topic:", repr(e))
    finally:
        admin.close()


def stream_forever(producer, stats):
    last_id = None
    backoff = 1
    session = requests.Session()
    while True:
        headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
        if last_id:
            headers["Last-Event-ID"] = last_id  # lanjut dari event terakhir
        try:
            with session.get(
                STREAM_URL, headers=headers, stream=True, timeout=(10, 60)
            ) as r:
                r.raise_for_status()
                r.encoding = "utf-8"
                backoff = 1
                print("Terhubung ke EventStreams.")
                for line in r.iter_lines(decode_unicode=True):
                    if not line:
                        continue
                    if line.startswith("id:"):
                        last_id = line[3:].strip()
                        continue
                    if not line.startswith("data:"):
                        continue
                    try:
                        ev = json.loads(line[5:])
                    except ValueError:
                        stats["bad"] += 1
                        continue
                    msg = transform(ev)
                    if msg is None:
                        stats["skip"] += 1
                        continue
                    producer.send(TOPIC, key=make_key(ev, msg), value=msg)
                    stats["sent"] += 1
                    report(stats)
                    if stats["sent"] % 200 == 0 and os.path.exists(STOP_FILE):
                        raise KeyboardInterrupt
        except requests.RequestException as e:
            stats["reconnect"] += 1
            print("Koneksi putus (%s). Reconnect dalam %ds..." % (type(e).__name__, backoff))
        else:
            stats["reconnect"] += 1
            print("Stream berakhir. Reconnect dalam %ds..." % backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


def report(stats, force=False):
    now = time.time()
    if not force and now - stats["last_report"] < REPORT_EVERY_SEC:
        return
    elapsed = now - stats["start"]
    interval = max(now - stats["last_report"], 1e-9)
    rate_now = (stats["sent"] - stats["last_sent"]) / interval
    rate_avg = stats["sent"] / max(elapsed, 1e-9)
    print(
        "[%s] terkirim=%d  laju_interval=%.1f/detik  laju_rata2=%.1f/detik  "
        "json_rusak=%d  dilewati=%d  reconnect=%d"
        % (
            time.strftime("%H:%M:%S"),
            stats["sent"],
            rate_now,
            rate_avg,
            stats["bad"],
            stats["skip"],
            stats["reconnect"],
        )
    )
    stats["last_report"] = now
    stats["last_sent"] = stats["sent"]


def main():
    if not ANON_SALT:
        raise SystemExit("ANON_SALT belum diatur.")
    if os.path.exists(STOP_FILE):
        os.remove(STOP_FILE)
    from kafka import KafkaProducer

    ensure_topic()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        retries=5,
        linger_ms=50,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    now = time.time()
    stats = {"sent": 0, "bad": 0, "skip": 0, "reconnect": 0,
             "start": now, "last_report": now, "last_sent": 0}
    print("Producer jalan. Topic=%s  Bootstrap=%s  (Ctrl+C untuk berhenti)" % (TOPIC, BOOTSTRAP))
    try:
        stream_forever(producer, stats)
    except KeyboardInterrupt:
        print("\nDihentikan. Mengirim sisa pesan...")
    finally:
        producer.flush()
        producer.close()
        report(stats, force=True)
        print("Selesai.")


if __name__ == "__main__":
    main()
