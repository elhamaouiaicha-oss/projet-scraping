"""
bronze_to_silver.py — Transformation PySpark : Bronze → Silver
Filière IADATA · EMSI Casablanca · 2025-2026

Opérations Silver :
  - Suppression des balises HTML
  - Normalisation du texte (Unicode NFC)
  - Détection / validation de la langue
  - Parsing de la date de publication
  - Dédoublonnage par URL
  - Calcul du nombre de mots

Lancement :
  spark-submit --master spark://spark-master:7077 bronze_to_silver.py
  ou via Airflow (SparkSubmitOperator)
"""

import re
import unicodedata
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

# ─── Session Spark ────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("BronzeToSilver")
    .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",         "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key",         "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access",  "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─── UDFs ─────────────────────────────────────────────────────────────────────

def _clean_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    text = re.sub(r"&#\d+;", " ", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def _normalize_unicode(text: str) -> str:
    """Normalisation Unicode NFC — important pour l'arabe et le français."""
    if not text:
        return ""
    return unicodedata.normalize("NFC", text)


def _word_count(text: str) -> int:
    if not text:
        return 0
    return len(text.split())


def _parse_date(date_str: str) -> str:
    """Tente de parser différents formats de date."""
    if not date_str:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
    ):
        try:
            return datetime.strptime(date_str[:19], fmt[:len(date_str[:19])]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            continue
    return date_str  # retourne la chaîne brute si aucun format ne correspond


clean_html_udf   = F.udf(_clean_html,       StringType())
normalize_udf    = F.udf(_normalize_unicode, StringType())
word_count_udf   = F.udf(_word_count,        IntegerType())
parse_date_udf   = F.udf(_parse_date,        StringType())

# ─── Pipeline ─────────────────────────────────────────────────────────────────

def run(bronze_path: str, silver_path: str):
    print(f"[Silver] Lecture Bronze : {bronze_path}")

    # 1. Lecture des données brutes Bronze
    df = (
        spark.read
        .option("multiline", "true")
        .json(bronze_path)
    )

    print(f"[Silver] Articles Bronze lus : {df.count()}")

    # 2. Nettoyage HTML + normalisation Unicode
    df = (
        df
        .withColumn("title",   normalize_udf(clean_html_udf(F.col("title"))))
        .withColumn("content", normalize_udf(clean_html_udf(F.col("content"))))
        .withColumn("author",  normalize_udf(clean_html_udf(F.col("author"))))
        .withColumn("category",normalize_udf(F.col("category")))
    )

    # 3. Parsing de la date
    df = df.withColumn("published_at_parsed", parse_date_udf(F.col("published_at")))

    # 4. Calcul du nombre de mots
    df = df.withColumn("word_count", word_count_udf(F.col("content")))

    # 5. Filtrage des articles invalides
    df = df.filter(
        (F.col("title").isNotNull())    & (F.length("title") > 5) &
        (F.col("content").isNotNull())  & (F.length("content") > 100) &
        (F.col("word_count") >= 50)
    )

    # 6. Dédoublonnage sur l'URL
    df = df.dropDuplicates(["url"])

    # 7. Ajout des métadonnées Silver
    df = (
        df
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("tier", F.lit("silver"))
    )

    print(f"[Silver] Articles après filtrage/dédup : {df.count()}")

    # 8. Écriture en Parquet partitionné par source et date
    (
        df
        .write
        .mode("append")
        .partitionBy("source", "language")
        .parquet(silver_path)
    )

    print(f"[Silver] Écriture terminée → {silver_path}")
    return df


if __name__ == "__main__":
    BRONZE = "s3a://bronze/2024/01/01/00/*/*.json"   # adapté par Airflow
    SILVER = "s3a://silver/"

    run(BRONZE, SILVER)
    spark.stop()
