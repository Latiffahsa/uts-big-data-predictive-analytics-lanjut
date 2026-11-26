"""Pengaturan bersama semua skrip UTS ST153 (Wikipedia recentchange -> Kafka -> Spark)."""
import os
import sys
import time

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "wiki.recentchange.raw")
SNAPSHOT_PATH = os.environ.get(
    "SNAPSHOT_PATH", os.path.abspath(os.path.join("data", "wiki_snapshot.jsonl"))
)
OUT_DIR = os.environ.get("OUT_DIR", os.path.abspath("out"))
# local[*] = Spark mode lokal. Untuk cluster: set SPARK_MASTER=spark://IP_MASTER:7077
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)


def spark_path(p):
    """Path lokal Windows -> URI yang aman untuk Spark."""
    if os.name == "nt" and ":" in p and not p.startswith("file:"):
        return "file:///" + p.replace("\\", "/")
    return p


def make_spark(app_name, kafka=False, shuffle_partitions=None, extra=None):
    import pyspark
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).master(SPARK_MASTER)
    if kafka:
        # Spark 4.x memakai Scala 2.13; versi konektor harus sama dengan versi pyspark
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:%s" % pyspark.__version__,
        )
    if shuffle_partitions is not None:
        builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    drv = os.environ.get("DRIVER_HOST")  # IP PC Master (driver) agar Worker bisa menghubungi driver
    if drv and SPARK_MASTER.startswith("spark://"):
        builder = builder.config("spark.driver.host", drv).config("spark.driver.bindAddress", "0.0.0.0")
    for k, v in (extra or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def wait_for_ui():
    """Tahan Spark UI agar bisa di-screenshot. UI_SECONDS=90 (tanpa terminal interaktif) atau tekan Enter."""
    secs = int(os.environ.get("UI_SECONDS", "0"))
    if secs > 0:
        print("\nSpark UI (http://localhost:4040) aktif %d detik lagi. Ambil screenshot sekarang." % secs)
        time.sleep(secs)
    elif os.environ.get("KEEP_UI", "1") == "1" and sys.stdin and sys.stdin.isatty():
        input("\nSpark UI (http://localhost:4040) masih aktif. Ambil screenshot, lalu tekan Enter untuk menutup...")


def edit_schema():
    """Skema eksplisit untuk event recentchange yang dikirim producer."""
    from pyspark.sql.types import (
        BooleanType, IntegerType, LongType, StringType, StructField, StructType,
    )

    return StructType([
        StructField("id", LongType()),
        StructField("type", StringType()),
        StructField("wiki", StringType()),
        StructField("server_name", StringType()),
        StructField("title", StringType()),
        StructField("namespace", IntegerType()),
        StructField("user", StringType()),
        StructField("bot", BooleanType()),
        StructField("minor", BooleanType()),
        StructField("timestamp", LongType()),
        StructField("length", StructType([
            StructField("old", LongType()),
            StructField("new", LongType()),
        ])),
        StructField("comment", StringType()),
    ])
