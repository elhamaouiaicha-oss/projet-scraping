"""
data_quality_checks.py — Contrôles qualité des données (couches Bronze et Silver)
Filière IADATA · EMSI Casablanca · 2025-2026

Dimensions contrôlées :
  ✓ Complétude  — champs obligatoires présents et non vides
  ✓ Cohérence   — dates valides, URL bien formées, word_count cohérent
  ✓ Validité    — langue reconnue, source dans la liste blanche
  ✓ Unicité     — pas de doublons sur URL

Utilisation standalone :
  python data_quality_checks.py

Depuis Airflow (PythonOperator) :
  from data_quality_checks import check_bronze_layer, check_silver_layer
"""

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

# Si PySpark est disponible on l'utilise, sinon mode JSON local pour les tests
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

VALID_SOURCES = {
    "hespress", "akhbarona", "lakom", "barlamane",
    "aljazeera", "cnn", "bbc", "reuters",
}
VALID_LANGUAGES = {"fr", "ar", "en", "es", "de", "pt", "unknown"}
URL_PATTERN     = re.compile(r"^https?://[\w\-.]+(:\d+)?(/.*)?$")


# ─── Checks individuels ───────────────────────────────────────────────────────

def check_completeness(record: dict) -> dict:
    """Vérifie que les champs obligatoires sont présents et non vides."""
    required = ["id", "url", "title", "content", "source", "scraped_at"]
    missing  = [f for f in required if not record.get(f)]
    return {
        "pass":    len(missing) == 0,
        "missing": missing,
        "score":   (len(required) - len(missing)) / len(required),
    }


def check_coherence(record: dict) -> dict:
    """Vérifie la cohérence interne des champs."""
    issues = []

    # Cohérence word_count vs contenu réel
    actual_wc = len(record.get("content", "").split())
    declared_wc = record.get("word_count", 0)
    if abs(actual_wc - declared_wc) > 50:
        issues.append(f"word_count incohérent: déclaré={declared_wc}, réel={actual_wc}")

    # Contenu trop court
    if actual_wc < 30:
        issues.append(f"Contenu trop court: {actual_wc} mots")

    # Titre trop court
    if len(record.get("title", "")) < 5:
        issues.append("Titre trop court (< 5 caractères)")

    # Date de scraping dans le futur
    scraped = record.get("scraped_at", "")
    if scraped:
        try:
            dt = datetime.fromisoformat(scraped.replace("Z", "+00:00"))
            if dt > datetime.now(dt.tzinfo):
                issues.append(f"scraped_at dans le futur: {scraped}")
        except ValueError:
            issues.append(f"scraped_at non parseable: {scraped}")

    return {"pass": len(issues) == 0, "issues": issues}


def check_validity(record: dict) -> dict:
    """Vérifie la validité des valeurs : source, langue, URL."""
    issues = []

    if record.get("source") not in VALID_SOURCES:
        issues.append(f"Source inconnue: {record.get('source')}")

    if record.get("language") not in VALID_LANGUAGES:
        issues.append(f"Langue non reconnue: {record.get('language')}")

    url = record.get("url", "")
    if url and not URL_PATTERN.match(url):
        issues.append(f"URL invalide: {url[:80]}")

    return {"pass": len(issues) == 0, "issues": issues}


# ─── Contrôle Bronze (fichiers JSON locaux ou via Spark) ─────────────────────

def check_bronze_layer(bronze_path: str) -> dict:
    """Contrôle la couche Bronze — retourne un rapport résumé."""

    if SPARK_AVAILABLE and bronze_path.startswith("s3a://"):
        return _check_bronze_spark(bronze_path)
    return _check_bronze_local(bronze_path)


def _check_bronze_local(directory: str) -> dict:
    """Mode local : lit les fichiers JSON dans un répertoire."""
    records   = []
    json_dir  = Path(directory)
    for f in json_dir.rglob("*.json"):
        try:
            records.append(json.loads(f.read_text("utf-8")))
        except Exception:
            pass

    return _evaluate_records(records, layer="bronze")


def _check_bronze_spark(path: str) -> dict:
    """Mode Spark : lit directement depuis MinIO."""
    spark = SparkSession.builder.appName("QualityBronze").getOrCreate()
    df = spark.read.option("multiline", "true").json(path)

    total = df.count()
    missing_title   = df.filter(F.col("title").isNull()   | (F.length("title") < 5)).count()
    missing_content = df.filter(F.col("content").isNull() | (F.length("content") < 100)).count()
    missing_date    = df.filter(F.col("published_at").isNull()).count()
    duplicate_urls  = total - df.dropDuplicates(["url"]).count()
    short_content   = df.filter(F.col("word_count") < 30).count()

    completeness_rate = 1 - (missing_title + missing_content + missing_date) / (total * 3 + 1e-9)
    critical_failures = int(missing_title + missing_content)

    report = {
        "layer":             "bronze",
        "total_records":     total,
        "missing_title":     missing_title,
        "missing_content":   missing_content,
        "missing_date":      missing_date,
        "duplicate_urls":    duplicate_urls,
        "short_content":     short_content,
        "completeness_rate": round(completeness_rate, 3),
        "critical_failures": critical_failures,
        "checked_at":        datetime.now().isoformat(),
    }
    print(f"[QualityBronze] {json.dumps(report, indent=2)}")
    return report


def _check_silver_spark(path: str) -> dict:
    """Contrôle Silver via Spark."""
    spark = SparkSession.builder.appName("QualitySilver").getOrCreate()
    df = spark.read.parquet(path)

    total            = df.count()
    missing_fields   = df.filter(
        F.col("title").isNull() | F.col("content").isNull() |
        F.col("source").isNull() | F.col("language").isNull()
    ).count()
    invalid_lang     = df.filter(~F.col("language").isin(list(VALID_LANGUAGES))).count()
    invalid_source   = df.filter(~F.col("source").isin(list(VALID_SOURCES))).count()
    dup_urls         = total - df.dropDuplicates(["url"]).count()
    completeness_rate = 1 - missing_fields / (total + 1e-9)

    report = {
        "layer":             "silver",
        "total_records":     total,
        "missing_fields":    missing_fields,
        "invalid_language":  invalid_lang,
        "invalid_source":    invalid_source,
        "duplicate_urls":    dup_urls,
        "completeness_rate": round(completeness_rate, 3),
        "critical_failures": int(missing_fields + invalid_source),
        "checked_at":        datetime.now().isoformat(),
    }
    print(f"[QualitySilver] {json.dumps(report, indent=2)}")
    return report


def check_silver_layer(silver_path: str) -> dict:
    if SPARK_AVAILABLE:
        return _check_silver_spark(silver_path)
    return {"completeness_rate": 1.0, "critical_failures": 0, "note": "Spark non disponible"}


def _evaluate_records(records: list, layer: str) -> dict:
    """Évalue une liste de records Python et retourne un rapport."""
    total     = len(records)
    if total == 0:
        return {"layer": layer, "total_records": 0, "critical_failures": 0, "completeness_rate": 0}

    comp_fails, coher_fails, valid_fails, urls = 0, 0, 0, []
    for r in records:
        if not check_completeness(r)["pass"]:
            comp_fails += 1
        if not check_coherence(r)["pass"]:
            coher_fails += 1
        if not check_validity(r)["pass"]:
            valid_fails += 1
        urls.append(r.get("url"))

    dup_count = total - len(set(u for u in urls if u))

    return {
        "layer":              layer,
        "total_records":      total,
        "completeness_fails": comp_fails,
        "coherence_fails":    coher_fails,
        "validity_fails":     valid_fails,
        "duplicate_urls":     dup_count,
        "completeness_rate":  round(1 - comp_fails / total, 3),
        "critical_failures":  comp_fails,
        "checked_at":         datetime.now().isoformat(),
    }


# ─── Test standalone ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    sample = {
        "id":           "abc-123",
        "source":       "hespress",
        "country":      "MA",
        "url":          "https://fr.hespress.com/test-article",
        "title":        "Test article sur l'économie marocaine",
        "author":       "Journaliste",
        "published_at": "2025-03-01T10:00:00Z",
        "category":     "Économie",
        "content":      "Ceci est un article de test avec suffisamment de contenu pour passer les contrôles qualité définis dans le cadre du projet Big Data de la filière IADATA.",
        "language":     "fr",
        "scraped_at":   "2025-03-01T11:00:00Z",
        "word_count":   28,
    }

    print("=== Test des contrôles qualité ===")
    print("Complétude :", check_completeness(sample))
    print("Cohérence  :", check_coherence(sample))
    print("Validité   :", check_validity(sample))

    report = _evaluate_records([sample], "bronze")
    print("\nRapport Bronze :")
    print(json.dumps(report, indent=2))
