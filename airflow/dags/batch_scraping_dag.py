"""
batch_scraping_dag.py — DAG Airflow : Pipeline de scraping batch
Filière IADATA · EMSI Casablanca · 2025-2026

Planification : toutes les heures
Étapes :
  1. scraping_task       — Lance le scraper BeautifulSoup (tous les sites)
  2. quality_bronze_task — Contrôles qualité sur les données Bronze
  3. bronze_to_silver    — Transformation PySpark Bronze → Silver
  4. quality_silver_task — Contrôles qualité Silver
  5. silver_to_gold      — Transformation PySpark Silver → Gold
  6. notify_task         — Notification de fin de pipeline
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ─── Paramètres par défaut ────────────────────────────────────────────────────

default_args = {
    "owner":            "iadata-team",
    "depends_on_past":  False,
    "email":            ["news-pipeline@emsi.ma"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

MINIO_BRONZE = "s3a://bronze/"
MINIO_SILVER = "s3a://silver/"
MINIO_GOLD   = "s3a://gold/"
DW_URL       = "jdbc:postgresql://datawarehouse:5432/newsdb"
SPARK_MASTER = "spark://spark-master:7077"
SPARK_JARS   = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.7.3"
)

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="batch_news_pipeline",
    description="Pipeline Big Data : scraping → Data Lake → Data Warehouse",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["scraping", "big-data", "iadata"],
) as dag:

    # ── Étape 1 : Scraping BeautifulSoup ──────────────────────────────────────
    scraping_task = BashOperator(
        task_id="scraping_batch",
        bash_command=(
            "cd /opt/airflow/scrapers && "
            "python scraper_bs4.py --mode=single-run"
        ),
        env={
            "MINIO_ENDPOINT":          "http://minio:9000",
            "MINIO_ACCESS_KEY":        "minioadmin",
            "MINIO_SECRET_KEY":        "minioadmin",
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        },
    )

    # ── Étape 2 : Contrôles qualité Bronze ────────────────────────────────────
    def run_quality_bronze(**context):
        import sys, os
        sys.path.insert(0, "/opt/airflow/quality")
        from data_quality_checks import check_bronze_layer

        run_date = context["ds"]
        path = f"s3a://bronze/{run_date.replace('-', '/')}/**/*.json"
        report = check_bronze_layer(path)

        if report["critical_failures"] > 0:
            raise ValueError(
                f"Qualité Bronze ÉCHOUÉE : {report['critical_failures']} "
                f"échecs critiques. Rapport: {report}"
            )
        print(f"✓ Qualité Bronze OK — {report}")
        return report

    quality_bronze_task = PythonOperator(
        task_id="quality_check_bronze",
        python_callable=run_quality_bronze,
        provide_context=True,
    )

    # ── Étape 3 : Transformation Bronze → Silver (Spark) ──────────────────────
    bronze_to_silver = BashOperator(
    task_id="bronze_to_silver",
    bash_command=(
        "spark-submit "
        "--master spark://spark-master:7077 "
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.7.3 "
        "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
        "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
        "--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
        "--conf spark.hadoop.fs.s3a.path.style.access=true "
        "--executor-memory 2g "
        "--driver-memory 1g "
        "--total-executor-cores 2 "
        "/opt/airflow/transformations/bronze_to_silver.py "
        "--bronze s3a://bronze/ "
        "--silver s3a://silver/"
    ),
)

    # ── Étape 4 : Contrôles qualité Silver ────────────────────────────────────
    def run_quality_silver(**context):
        import sys
        sys.path.insert(0, "/opt/airflow/quality")
        from data_quality_checks import check_silver_layer

        report = check_silver_layer(MINIO_SILVER)
        print(f"✓ Qualité Silver — {report}")

        if report["completeness_rate"] < 0.85:
            raise ValueError(
                f"Taux de complétude Silver trop bas : "
                f"{report['completeness_rate']:.1%}"
            )
        return report

    quality_silver_task = PythonOperator(
        task_id="quality_check_silver",
        python_callable=run_quality_silver,
        provide_context=True,
    )

    # ── Étape 5 : Transformation Silver → Gold (Spark) ────────────────────────
    silver_to_gold = BashOperator(
    task_id="silver_to_gold",
    bash_command=(
        "spark-submit "
        "--master spark://spark-master:7077 "
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.7.3 "
        "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
        "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
        "--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
        "--conf spark.hadoop.fs.s3a.path.style.access=true "
        "--executor-memory 3g "
        "--driver-memory 2g "
        "--total-executor-cores 4 "
        "/opt/airflow/transformations/silver_to_gold.py "
        "--silver s3a://silver/ "
        "--gold s3a://gold/ "
        "--dw jdbc:postgresql://datawarehouse:5432/newsdb"
    ),
)

    # ── Étape 6 : Notification de fin ─────────────────────────────────────────
    def notify_success(**context):
        run_date = context["ds"]
        print(
            f"✅ Pipeline terminé avec succès — date: {run_date}\n"
            f"   Airflow run_id: {context['run_id']}\n"
            f"   Dashboards Superset disponibles sur http://localhost:8088"
        )

    notify_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        provide_context=True,
    )

    # ─── Dépendances ──────────────────────────────────────────────────────────
    (
        scraping_task
        >> quality_bronze_task
        >> bronze_to_silver
        >> quality_silver_task
        >> silver_to_gold
        >> notify_task
    )
