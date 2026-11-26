"""
Grafik analisis (pengganti dashboard) dari hasil Soal 2, 3, dan 4. Hasil PNG di out/grafik/
Instal: pip install pandas matplotlib
Jalankan: python grafik_analisis.py   (setelah soal2, soal3, soal4 dijalankan)
"""
import glob
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from config import OUT_DIR

G = os.path.join(OUT_DIR, "grafik")
os.makedirs(G, exist_ok=True)


def save(fig, name):
    p = os.path.join(G, name)
    fig.tight_layout()
    fig.savefig(p, dpi=150)
    plt.close(fig)
    print("Disimpan:", p)


def main():
    p = os.path.join(OUT_DIR, "s2_hasil.json")
    if os.path.exists(p):
        d = json.load(open(p, encoding="utf-8"))
        tw = d["top_wiki_manusia"]
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.barh([w for w, _ in tw][::-1], [c for _, c in tw][::-1])
        ax.set_title("10 wiki dengan edit manusia terbanyak (RDD, Soal 2)")
        ax.set_xlabel("jumlah edit")
        save(fig, "1_top_wiki_manusia.png")

    p = os.path.join(OUT_DIR, "s3_bot_persen.csv")
    if os.path.exists(p):
        df = pd.read_csv(p).head(10)
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.bar(df["wiki"], df["persen_manusia"], label="manusia")
        ax.bar(df["wiki"], df["persen_bot"], bottom=df["persen_manusia"], label="bot")
        ax.set_title("Persentase manusia vs bot per wiki (SQL, Soal 3)")
        ax.set_ylabel("%")
        plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
        ax.legend()
        save(fig, "2_bot_vs_manusia.png")

    p = os.path.join(OUT_DIR, "s3_edits_per_menit.csv")
    if os.path.exists(p):
        df = pd.read_csv(p, parse_dates=["menit"])
        pv = df.pivot_table(index="menit", columns="wiki", values="jumlah_edit", aggfunc="sum")
        fig, ax = plt.subplots(figsize=(9, 4.5))
        pv.plot(ax=ax)
        ax.set_title("Jumlah edit per menit pada 5 wiki teratas (SQL, Soal 3)")
        ax.set_ylabel("edit/menit")
        save(fig, "3_edit_per_menit.png")

    p = os.path.join(OUT_DIR, "s3_lonjakan.csv")
    if os.path.exists(p):
        df = pd.read_csv(p).head(10)
        if len(df):
            fig, ax = plt.subplots(figsize=(9, 4.5))
            labels = [("%s: %s" % (w, str(t)[:30])) for w, t in zip(df["wiki"], df["title"])]
            ax.barh(labels[::-1], df["n_edit"][::-1])
            ax.set_title("Halaman dengan lonjakan edit tertinggi (jendela 5 menit)")
            ax.set_xlabel("jumlah edit dalam 5 menit")
            save(fig, "4_lonjakan.png")

    parts = glob.glob(os.path.join(OUT_DIR, "stream_batches", "*.csv"))
    if parts:
        df = pd.concat([pd.read_csv(x) for x in parts], ignore_index=True)
        df = df.sort_values("batch_id").drop_duplicates(["tag", "window_start", "wiki"], keep="last")
        top = df.groupby("wiki")["n_edit"].sum().nlargest(5).index
        sub = df[df["wiki"].isin(top)].copy()
        sub["window_start"] = pd.to_datetime(sub["window_start"])
        pv = sub.pivot_table(index="window_start", columns="wiki", values="n_edit", aggfunc="sum")
        fig, ax = plt.subplots(figsize=(9, 4.5))
        pv.plot(ax=ax)
        ax.set_title("Edit per jendela 1 menit, 5 wiki teratas (Structured Streaming, Soal 4)")
        ax.set_ylabel("n_edit")
        save(fig, "5_streaming_per_jendela.png")
        rb = sub.groupby("wiki")["rasio_bot"].mean().sort_values()
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.barh(rb.index, rb.values)
        ax.set_title("Rata-rata rasio bot per wiki (streaming)")
        save(fig, "6_streaming_rasio_bot.png")
    print("Selesai.")


if __name__ == "__main__":
    main()
