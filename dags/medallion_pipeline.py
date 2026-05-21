from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "medallion_architecture_pipeline",
    default_args=default_args,
    description="Orchestrates Bronze -> Silver -> Gold Delta Lake pipeline",
    schedule_interval="@daily",
    start_date=days_ago(1),
    tags=["delta-lake", "medallion", "spark"],
)

# Команды для spark-submit (предполагаем, что spark-submit есть в PATH)
# В production здесь использовался бы SparkSubmitOperator с кластером
SUBMIT_CMD = "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0"

t1_bronze = BashOperator(
    task_id="run_bronze_ingestion",
    bash_command=f"{SUBMIT_CMD} /opt/airflow/scripts/bronze_ingest.py",
    dag=dag,
)

t2_silver = BashOperator(
    task_id="run_silver_cleaning",
    bash_command=f"{SUBMIT_CMD} /opt/airflow/scripts/silver_clean.py",
    dag=dag,
)

t3_gold = BashOperator(
    task_id="run_gold_aggregation",
    bash_command=f"{SUBMIT_CMD} /opt/airflow/scripts/gold_aggregate.py",
    dag=dag,
)

# Зависимости
t1_bronze >> t2_silver >> t3_gold
