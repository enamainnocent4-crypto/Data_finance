from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

# ==================================================
# CONFIGURATION
# ==================================================
SPARK_SUBMIT_CMD = """
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.cores.max=4 \
    --conf spark.executor.cores=2 \
    --conf spark.executor.memory=1g \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.5.0 \
    --jars /opt/spark/jars/spark-snowflake_2.12-3.1.7.jar,/opt/spark/jars/snowflake-jdbc-3.13.30.jar \
    /tmp/fraud_stream.py
"""

COPY_SCRIPT_CMD = """
docker cp /opt/airflow/processing/fraud_stream.py spark-master:/tmp/fraud_stream.py
"""

CHECK_SPARK_CMD = """
docker exec spark-master /opt/spark/bin/spark-submit --version
"""

# ==================================================
# DEFAULT ARGS
# ==================================================
default_args = {
    "owner": "enama",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ==================================================
# DAG
# ==================================================
with DAG(
    dag_id="fraud_detection_pipeline",
    default_args=default_args,
    description="Pipeline de détection de fraude : Kafka → Spark → PostgreSQL + Snowflake",
    schedule_interval="*/30 * * * *",  # Toutes les 30 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=["fraud", "spark", "kafka", "snowflake"],
) as dag:

    # --------------------------------------------------
    # TÂCHE 1 : Vérifier que Spark est disponible
    # --------------------------------------------------
    check_spark = BashOperator(
        task_id="check_spark_master",
        bash_command=CHECK_SPARK_CMD,
        retries=3,
        retry_delay=timedelta(seconds=30),
    )

    # --------------------------------------------------
    # TÂCHE 2 : Copier le script dans le conteneur Spark
    # --------------------------------------------------
    copy_script = BashOperator(
        task_id="copy_fraud_stream_script",
        bash_command=COPY_SCRIPT_CMD,
    )

    # --------------------------------------------------
    # TÂCHE 3 : Lancer le job Spark (durée limitée)
    # --------------------------------------------------
    run_spark_job = BashOperator(
        task_id="run_spark_fraud_detection",
        bash_command=SPARK_SUBMIT_CMD,
        execution_timeout=timedelta(minutes=25),  # Stop avant le prochain run
    )

    # --------------------------------------------------
    # TÂCHE 4 : Vérifier les données dans PostgreSQL
    # --------------------------------------------------
    check_postgres = BashOperator(
        task_id="check_postgres_data",
        bash_command="""
        docker exec postgres psql -U admin -d fraud_db -c \
        "SELECT status, COUNT(*) as nb FROM fraud_transactions GROUP BY status ORDER BY nb DESC;"
        """,
    )

    # --------------------------------------------------
    # ORDRE D'EXÉCUTION
    # --------------------------------------------------
    check_spark >> copy_script >> run_spark_job >> check_postgres
