"""
pipelines.py — Scrapy Pipelines : Kafka (streaming) + MinIO (batch Bronze)
"""

import io
import json
import os
from datetime import datetime, timezone

from confluent_kafka import Producer
from minio import Minio


class KafkaPipeline:
    """Envoie chaque article scrapy comme événement Kafka (streaming)."""

    def open_spider(self, spider):
        self.producer = Producer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "acks": "all",
        })
        self.topic = os.getenv("KAFKA_TOPIC_ARTICLES", "news-articles-raw")

    def process_item(self, item, spider):
        payload = json.dumps(dict(item), ensure_ascii=False).encode("utf-8")
        self.producer.produce(topic=self.topic, key=item["id"].encode(), value=payload)
        self.producer.poll(0)
        return item

    def close_spider(self, spider):
        self.producer.flush()


class MinioPipeline:
    """Persiste chaque article en JSON dans MinIO — bucket bronze."""

    def open_spider(self, spider):
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "")
        self.client = Minio(
            endpoint,
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )

    def process_item(self, item, spider):
        now  = datetime.now(timezone.utc)
        key  = f"{now.strftime('%Y/%m/%d/%H')}/{item['source']}/{item['id']}.json"
        data = json.dumps(dict(item), ensure_ascii=False, indent=2).encode("utf-8")
        self.client.put_object(
            bucket_name="bronze",
            object_name=key,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
        return item
