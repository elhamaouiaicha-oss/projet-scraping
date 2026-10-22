"""
data_catalog.py — Gouvernance & Catalogue de données
Filière IADATA · EMSI Casablanca · 2025-2026

Ce module assure :
  - Catalogue des datasets (métadonnées, schémas, responsables)
  - Traçabilité du pipeline (lineage)
  - Logging des transformations
  - Génération de rapport de gouvernance
"""

import json
import os
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional

import psycopg2

# ─── Catalogue des datasets ────────────────────────────────────────────────────

CATALOG = {
    "bronze": {
        "name":        "Bronze — Articles bruts",
        "description": "Articles de presse collectés bruts, sans transformation. "
                       "Conservés indéfiniment comme source de vérité.",
        "location":    "s3a://bronze/",
        "format":      "JSON",
        "partition":   "AAAA/MM/JJ/HH/source/",
        "owner":       "team-ingestion",
        "update_freq": "Toutes les heures",
        "schema": {
            "id":           "UUID — identifiant unique de l'article",
            "source":       "str — nom du site source",
            "country":      "str(2) — code pays ISO",
            "url":          "str — URL originale",
            "title":        "str — titre brut",
            "author":       "str — auteur",
            "published_at": "str — date de publication (format variable)",
            "category":     "str — catégorie/rubrique",
            "content":      "str — contenu textuel brut",
            "language":     "str — langue détectée (ISO 639-1)",
            "scraped_at":   "datetime ISO-8601 UTC — horodatage de scraping",
            "word_count":   "int — nombre de mots",
        },
        "quality_rules": [
            "title non vide",
            "content >= 30 mots",
            "url valide (http/https)",
            "source dans la liste blanche",
        ],
    },
    "silver": {
        "name":        "Silver — Articles nettoyés",
        "description": "Articles après nettoyage HTML, normalisation Unicode, "
                       "dédoublonnage, et validation de langue.",
        "location":    "s3a://silver/",
        "format":      "Parquet",
        "partition":   "source/language/",
        "owner":       "team-transformation",
        "update_freq": "Toutes les heures (batch Spark)",
        "schema": {
            "id":                   "UUID",
            "source":               "str",
            "country":              "str(2)",
            "url":                  "str (dédoublonné)",
            "title":                "str (nettoyé HTML, normalisé NFC)",
            "author":               "str",
            "published_at_parsed":  "timestamp (standardisé)",
            "category":             "str",
            "content":              "str (nettoyé HTML, normalisé)",
            "language":             "str",
            "word_count":           "int",
            "silver_processed_at":  "timestamp",
            "tier":                 "str = 'silver'",
        },
    },
    "gold": {
        "name":        "Gold — Tables analytiques",
        "description": "Agrégats et KPIs prêts pour la visualisation et la BI.",
        "location":    "s3a://gold/",
        "format":      "Parquet + PostgreSQL",
        "owner":       "team-analytics",
        "update_freq": "Toutes les heures",
        "tables": [
            "articles_by_day",
            "articles_by_theme",
            "articles_by_country",
            "articles_by_language",
            "top_keywords",
            "trending_sources",
        ],
    },
}

# ─── Lineage (traçabilité) ─────────────────────────────────────────────────────

@dataclass
class LineageEvent:
    event_id:      str          = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline:      str          = ""
    step:          str          = ""
    source_layer:  str          = ""
    target_layer:  str          = ""
    records_in:    int          = 0
    records_out:   int          = 0
    dropped:       int          = 0
    duration_sec:  float        = 0.0
    status:        str          = "success"   # success | failure | partial
    error_message: Optional[str]= None
    extra:         dict         = field(default_factory=dict)
    timestamp:     str          = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        return asdict(self)


class DataGovernance:
    """Gestionnaire de gouvernance : catalogue + traçabilité + reporting."""

    def __init__(self, dw_dsn: str = None):
        self.dw_dsn    = dw_dsn or os.getenv(
            "DW_CONN",
            "host=localhost port=5432 dbname=newsdb user=dwuser password=dwpass"
        )
        self._lineage: list[LineageEvent] = []

    # ── Catalogue ──────────────────────────────────────────────────────────────

    def describe_dataset(self, layer: str) -> dict:
        return CATALOG.get(layer, {"error": f"Couche '{layer}' inconnue"})

    def list_datasets(self) -> list[str]:
        return list(CATALOG.keys())

    # ── Lineage ────────────────────────────────────────────────────────────────

    def log_event(self, event: LineageEvent):
        self._lineage.append(event)
        print(
            f"[Lineage] {event.timestamp} | {event.pipeline}.{event.step} | "
            f"{event.source_layer}→{event.target_layer} | "
            f"in={event.records_in} out={event.records_out} "
            f"dropped={event.dropped} | status={event.status}"
        )
        self._persist_event(event)

    def _persist_event(self, event: LineageEvent):
        try:
            conn = psycopg2.connect(self.dw_dsn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO quality_reports
                      (layer, run_date, total_records, completeness_rate,
                       critical_failures, report_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        event.target_layer,
                        event.timestamp[:10],
                        event.records_out,
                        round(1 - event.dropped / max(event.records_in, 1), 4),
                        event.dropped if event.status != "success" else 0,
                        json.dumps(event.to_dict()),
                    ),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[Lineage] Erreur persistence: {e}")

    def get_lineage_report(self) -> list[dict]:
        return [e.to_dict() for e in self._lineage]

    # ── Rapport de gouvernance ─────────────────────────────────────────────────

    def generate_governance_report(self) -> dict:
        return {
            "generated_at":  datetime.now(timezone.utc).isoformat(),
            "project":       "Plateforme Big Data — Scraping Presse",
            "school":        "EMSI Casablanca",
            "program":       "IADATA 2025-2026",
            "catalog":       {k: {"name": v["name"], "owner": v.get("owner")}
                              for k, v in CATALOG.items()},
            "lineage_events": self.get_lineage_report(),
            "pipeline_steps": [
                {"step": "1. Collecte",        "tool": "BeautifulSoup / Scrapy"},
                {"step": "2. Ingestion",       "tool": "Kafka + MinIO (Bronze)"},
                {"step": "3. Transformation",  "tool": "PySpark (Silver → Gold)"},
                {"step": "4. Orchestration",   "tool": "Apache Airflow"},
                {"step": "5. Qualité",         "tool": "Custom + Spark checks"},
                {"step": "6. Data Warehouse",  "tool": "PostgreSQL"},
                {"step": "7. Visualisation",   "tool": "Apache Superset"},
            ],
            "data_sources": [
                {"site": "Hespress",  "country": "MA", "lang": "fr/ar"},
                {"site": "Akhbarona","country": "MA", "lang": "ar"},
                {"site": "Lakom",    "country": "MA", "lang": "ar/fr"},
                {"site": "Barlamane","country": "MA", "lang": "ar/fr"},
                {"site": "Al Jazeera","country": "QA","lang": "ar"},
                {"site": "CNN",      "country": "US", "lang": "en"},
                {"site": "BBC",      "country": "GB", "lang": "en"},
                {"site": "Reuters",  "country": "US", "lang": "en"},
            ],
        }

    def save_report(self, path: str = "governance_report.json"):
        report = self.generate_governance_report()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"[Governance] Rapport sauvegardé → {path}")
        return report


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    gov = DataGovernance()

    print("=== Catalogue de données ===")
    for layer in gov.list_datasets():
        info = gov.describe_dataset(layer)
        print(f"\n[{layer.upper()}] {info['name']}")
        print(f"  Localisation : {info.get('location', 'N/A')}")
        print(f"  Format       : {info.get('format', 'N/A')}")
        print(f"  Propriétaire : {info.get('owner', 'N/A')}")
        print(f"  Fréquence    : {info.get('update_freq', 'N/A')}")

    # Exemple d'événement de lineage
    event = LineageEvent(
        pipeline="batch_news_pipeline",
        step="bronze_to_silver",
        source_layer="bronze",
        target_layer="silver",
        records_in=1200,
        records_out=1150,
        dropped=50,
        duration_sec=45.3,
        status="success",
    )
    gov.log_event(event)

    gov.save_report("governance_report.json")
