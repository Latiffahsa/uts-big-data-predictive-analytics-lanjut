"""
Soal 4c - Verifikasi restart dengan checkpoint yang sama:
 - offset awal sesi 2 harus = offset akhir sesi 1 (tidak ada data terlewat)
 - tidak ada offset yang diproses dua kali (tidak ganda)
 - jumlah baris input semua batch = selisih offset total
Pakai: python soal4c_verifikasi.py run1 run2
"""
import json
import os
import sys

from config import OUT_DIR


def load(tag):
    path = os.path.join(OUT_DIR, "progress_%s.jsonl" % tag)
    items = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                items.append(json.loads(line))
    batches = []
    for p in items:
        if p.get("numInputRows", 0) <= 0 or not p.get("sources"):
            continue
        s = p["sources"][0]
        try:
            start = json.loads(s["startOffset"]) if isinstance(s["startOffset"], str) else s["startOffset"]
            end = json.loads(s["endOffset"]) if isinstance(s["endOffset"], str) else s["endOffset"]
        except (TypeError, ValueError, KeyError):
            continue
        topic = next(iter(end))
        batches.append({"batch": p["batchId"], "rows": p["numInputRows"], "start": start[topic], "end": end[topic],
                        "ts": p["timestamp"]})
    batches.sort(key=lambda b: (b["ts"]))
    return batches


def main():
    tags = sys.argv[1:] or ["run1", "run2"]
    allb = []
    for t in tags:
        bs = load(t)
        print("Sesi %s: %d batch berisi data" % (t, len(bs)))
        for b in bs:
            print("   batch %-4s baris=%-6d offset awal=%s  akhir=%s" % (b["batch"], b["rows"], b["start"], b["end"]))
        allb.append((t, bs))
    ok = True
    total_rows = 0
    prev_end = None
    first_start = None
    for t, bs in allb:
        for b in bs:
            if prev_end is None:
                first_start = dict(b["start"])
            else:
                for p, s in b["start"].items():
                    if s != prev_end.get(p):
                        ok = False
                        print("!! Batch %s (%s) partisi %s: mulai di %s tapi batch sebelumnya berakhir di %s"
                              % (b["batch"], t, p, s, prev_end.get(p)))
            prev_end = b["end"]
            total_rows += b["rows"]
    print("\n=== HASIL ===")
    if prev_end is None:
        print("Tidak ada batch berisi data. Pastikan producer berjalan selama sesi.")
        return
    delta = sum(prev_end[p] - first_start[p] for p in prev_end)
    print("Total baris input semua batch : %d" % total_rows)
    print("Selisih offset (akhir - awal) : %d" % delta)
    print("Sambungan offset antar batch/sesi mulus (tidak terlewat, tidak ganda): %s" % ("YA" if ok else "TIDAK"))
    print("Jumlah baris = selisih offset : %s" % ("YA" if total_rows == delta else "TIDAK"))
    if len(allb) >= 2 and allb[0][1] and allb[1][1]:
        e1 = allb[0][1][-1]["end"]
        s2 = allb[1][1][0]["start"]
        print("Offset akhir sesi %s: %s" % (allb[0][0], e1))
        print("Offset awal  sesi %s: %s  -> %s" % (allb[1][0], s2, "SAMA (lanjut dari checkpoint)" if e1 == s2 else "BEDA (cek!)"))


if __name__ == "__main__":
    main()
