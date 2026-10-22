"""
silver_to_gold.py — Transformation PySpark : Silver → Gold
Filière IADATA · EMSI Casablanca · 2025-2026

Tables Gold générées :
  - articles_by_day       : volume d'articles par jour et par source
  - articles_by_theme     : top catégories par source et langue
  - articles_by_country   : répartition géographique
  - top_keywords          : mots-clés les plus fréquents (TF simple)
  - trending_sources      : tendances de volume par source sur 7 jours
  - articles_by_language  : répartition linguistique

Lancement :
  spark-submit --master spark://spark-master:7077 silver_to_gold.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# ─── Session Spark ────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("SilverToGold")
    .config("spark.hadoop.fs.s3a.endpoint",         "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",        "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─── Stopwords multilingues (FR + EN + AR basique) ───────────────────────────

STOPWORDS = {
    "fr": {"le","la","les","un","une","des","et","en","de","du","au","aux",
            "ce","se","son","sa","ses","que","qui","par","sur","pour","dans",
            "est","ont","pas","plus","ou","leur","leurs","il","elle","ils","a"},
    "en": {"the","a","an","and","or","in","on","of","to","for","is","are","was",
            "were","be","been","it","this","that","with","from","by","as","at"},
    "ar": {"في","من","إلى","على","أن","ما","هذا","هذه","مع","بعد","قبل",
            "عن","كان","التي","الذي","لا","أو","إن","كما","حتى"},
}

ALL_STOPS = set().union(*STOPWORDS.values())

stopwords_broadcast = spark.sparkContext.broadcast(ALL_STOPS)


def extract_keywords(text: str, lang: str) -> list:
    """Extraction de mots-clés : retourne les 10 mots significatifs les plus longs."""
    if not text:
        return []
    stops = stopwords_broadcast.value
    words = [
        w.lower().strip(".,;:!?\"'()[]{}") for w in text.split()
        if len(w) > 4 and w.lower() not in stops and w.isalpha()
    ]
    # déduplique en préservant l'ordre
    seen, out = set(), []
    for w in words:
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out[:15]


extract_kw_udf = F.udf(extract_keywords, ArrayType(StringType()))


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def run(silver_path: str, gold_path: str, dw_url: str, dw_props: dict):
    print(f"[Gold] Lecture Silver : {silver_path}")

    df = spark.read.parquet(silver_path)
    df = df.withColumn(
        "pub_date",
        F.to_date(F.col("published_at_parsed").cast("string"), "yyyy-MM-dd HH:mm:ss")
    )
    df.cache()
    print(f"[Gold] Articles Silver : {df.count()}")

    # ── 1. Articles par jour ──────────────────────────────────────────────────
    articles_by_day = (
        df
        .groupBy("pub_date", "source", "country")
        .agg(F.count("*").alias("article_count"))
        .orderBy("pub_date", "source")
    )
    _write(articles_by_day, gold_path, "articles_by_day", dw_url, dw_props)

    # ── 2. Articles par thème (catégorie) ─────────────────────────────────────
    articles_by_theme = (
        df
        .filter(F.col("category").isNotNull() & (F.length("category") > 2))
        .groupBy("category", "source", "language")
        .agg(F.count("*").alias("article_count"))
        .orderBy(F.desc("article_count"))
    )
    _write(articles_by_theme, gold_path, "articles_by_theme", dw_url, dw_props)

    # ── 3. Articles par pays ──────────────────────────────────────────────────
    articles_by_country = (
        df
        .groupBy("country", "language")
        .agg(
            F.count("*").alias("article_count"),
            F.avg("word_count").alias("avg_word_count"),
        )
        .orderBy(F.desc("article_count"))
    )
    _write(articles_by_country, gold_path, "articles_by_country", dw_url, dw_props)

    # ── 4. Répartition par langue ─────────────────────────────────────────────
    by_language = (
        df
        .groupBy("language", "source")
        .agg(F.count("*").alias("article_count"))
        .orderBy(F.desc("article_count"))
    )
    _write(by_language, gold_path, "articles_by_language", dw_url, dw_props)

    # ── 5. Top mots-clés ──────────────────────────────────────────────────────
    df_kw = df.withColumn("keywords", extract_kw_udf(F.col("content"), F.col("language")))
    top_keywords = (
        df_kw
        .select("source", "language", "pub_date", F.explode("keywords").alias("keyword"))
        .groupBy("keyword", "source", "language")
        .agg(F.count("*").alias("frequency"))
        .orderBy(F.desc("frequency"))
        .limit(500)
    )
    _write(top_keywords, gold_path, "top_keywords", dw_url, dw_props)

    # ── 6. Tendances sources (7 jours glissants) ──────────────────────────────
    window_7d = Window.partitionBy("source").orderBy("pub_date").rowsBetween(-6, 0)
    trending = (
        articles_by_day
        .withColumn("rolling_7d_avg", F.avg("article_count").over(window_7d))
        .filter(F.col("pub_date") >= F.date_sub(F.current_date(), 30))
    )
    _write(trending, gold_path, "trending_sources", dw_url, dw_props)

    df.unpersist()
    print("[Gold] Toutes les tables Gold générées.")


def _write(df, gold_path: str, table_name: str, dw_url: str, dw_props: dict):
    """Écrit en Parquet Gold ET dans le Data Warehouse PostgreSQL."""
    path = f"{gold_path}/{table_name}"
    df.write.mode("overwrite").parquet(path)
    print(f"  [Gold] ✓ {table_name} → MinIO Gold")

    df.write.jdbc(
        url=dw_url,
        table=table_name,
        mode="overwrite",
        properties=dw_props,
    )
    print(f"  [Gold] ✓ {table_name} → PostgreSQL DW")


if __name__ == "__main__":
    DW_URL   = "jdbc:postgresql://datawarehouse:5432/newsdb"
    DW_PROPS = {"user": "dwuser", "password": "dwpass", "driver": "org.postgresql.Driver"}

    run(
        silver_path="s3a://silver/",
        gold_path="s3a://gold/",
        dw_url=DW_URL,
        dw_props=DW_PROPS,
    )
    spark.stop()
