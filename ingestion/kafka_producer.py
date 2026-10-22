"""
kafka_producer.py — Consommateur Kafka → Silver (streaming)
Filière IADATA · EMSI Casablanca · 2025-2026

Ce module consomme les articles bruts du topic Kafka et les écrit
en temps réel dans MinIO bucket silver (après nettoyage minimal).
"""

import io
import json
import logging
import os
import re

from confluent_kafka import Consumer, KafkaError
from minio import Minio

logger = logging.getLogger("kafka-consumer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC_ARTICLES", "news-articles-raw")
KAFKA_GROUP    = "silver-streaming-consumer"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def clean_text(text: str) -> str:
    """Nettoyage de base : suppression HTML, espaces multiples."""
    text = re.sub(r"<[^>]+>", " ", text)          # HTML tags
    text = re.sub(r"&\w+;", " ", text)             # HTML entities
    text = re.sub(r"[\r\n\t]+", " ", text)         # retours à la ligne
    text = re.sub(r"\s{2,}", " ", text)            # espaces multiples
    return text.strip()


def transform_bronze_to_silver(article: dict) -> dict:
    """Transformation minimale appliquée en streaming (Bronze → Silver)."""
    return {
        **article,
        "title":   clean_text(article.get("title", "")),
        "content": clean_text(article.get("content", "")),
        "author":  clean_text(article.get("author", "Inconnu")),
        "tier":    "silver",
    }


def save_silver(client: Minio, article: dict):
    key  = (
        f"{article.get('scraped_at', '')[:10].replace('-', '/')}/"
        f"{article['source']}/{article['id']}.json"
    )
    data = json.dumps(article, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        bucket_name="silver",
        object_name=key,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/json",
    )


def run():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id":          KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])

    minio = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

    logger.info(f"Consommateur Kafka démarré — topic: {KAFKA_TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue

            article_raw = json.loads(msg.value().decode("utf-8"))
            article_silver = transform_bronze_to_silver(article_raw)
            save_silver(minio, article_silver)
            logger.info(f"Silver ← [{article_silver['source']}] {article_silver['title'][:60]}…")

    except KeyboardInterrupt:
        logger.info("Arrêt du consommateur")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
