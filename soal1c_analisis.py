"""
Soal 1c - Analisis hasil uji decoupling: apakah ada pesan hilang atau ganda?
Membaca out/consumer_log.jsonl yang ditulis soal1c_consumer.py.
"""
import collections
import json
import os

from config import OUT_DIR

LOG = os.path.join(OUT_DIR, "consumer_log.jsonl")


def load(path):
    rows, committed, sessions = [], {}, []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            ev = d.get("event")
            if ev == "start":
                sessions.append(d["session"])
            elif ev == "committed":
                committed[(d["session"], d["p"])] = d["committed"]
            elif ev is None:
                rows.append(d)
    return rows, committed, sessions


def ranges(missing):
    out, start, prev = [], None, None
    for x in missing:
        if start is None:
            start = prev = x
        elif x == prev + 1:
            prev = x
        else:
            out.append((start, prev))
            start = prev = x
    if start is not None:
        out.append((start, prev))
    return out


def analyze(rows, committed, sessions):
    by_p = collections.defaultdict(list)
    for r in rows:
        by_p[r["p"]].append(r)
    total_missing = total_dup_off = 0
    print("=== Ringkasan per partisi ===")
    for p in sorted(by_p):
        rs = by_p[p]
        offs = [r["o"] for r in rs]
        uniq = sorted(set(offs))
        expected = uniq[-1] - uniq[0] + 1
        miss = sorted(set(range(uniq[0], uniq[-1] + 1)) - set(uniq))
        dup = len(offs) - len(uniq)
        total_missing += len(miss)
        total_dup_off += dup
        print("Partisi %d: baris=%d unik=%d rentang offset %d..%d (diharapkan %d) | HILANG=%d | GANDA(offset)=%d"
              % (p, len(offs), len(uniq), uniq[0], uniq[-1], expected, len(miss), dup))
        if miss:
            print("   offset hilang (rentang):", ranges(miss)[:10])
    print("\n=== Kesinambungan antar sesi (bukti lanjut dari offset terakhir) ===")
    ok_all = True
    for p in sorted(by_p):
        per_sess = collections.OrderedDict()
        for r in by_p[p]:
            s = per_sess.setdefault(r["session"], [r["o"], r["o"]])
            s[0] = min(s[0], r["o"])
            s[1] = max(s[1], r["o"])
        prev = None
        for sid, (first, last) in per_sess.items():
            c = committed.get((sid, p))
            print("Partisi %d | sesi %s | committed awal=%s | offset pertama=%d | terakhir=%d" % (p, sid, c, first, last))
            if prev is not None:
                ok = first == prev + 1
                ok_all = ok_all and ok
                print("   sambungan dari sesi sebelumnya: offset terakhir %d -> pertama %d : %s"
                      % (prev, first, "MULUS (lanjut dari offset terakhir)" if ok else "TIDAK MULUS (cek!)"))
            prev = last
    print("\n=== Duplikat tingkat bisnis (kunci wiki:id muncul pada >1 offset) ===")
    key_offsets = collections.defaultdict(set)
    for r in rows:
        if r.get("k"):
            key_offsets[r["k"]].add((r["p"], r["o"]))
    dup_keys = {k: v for k, v in key_offsets.items() if len(v) > 1}
    print("Jumlah kunci unik: %d | kunci ganda: %d" % (len(key_offsets), len(dup_keys)))
    for k in list(dup_keys)[:5]:
        print("   contoh:", k, sorted(dup_keys[k]))
    print("\n=== KESIMPULAN ===")
    print("Pesan hilang (celah offset)      : %d" % total_missing)
    print("Pesan ganda (offset terbaca >1x) : %d" % total_dup_off)
    print("Sambungan antar sesi mulus       : %s" % ("YA" if ok_all else "TIDAK"))
    print("Kunci wiki:id ganda              : %d (bisa berasal dari sumber/reconnect producer)" % len(dup_keys))


if __name__ == "__main__":
    if not os.path.exists(LOG):
        raise SystemExit("File %s belum ada. Jalankan soal1c_consumer.py dulu." % LOG)
    analyze(*load(LOG))
