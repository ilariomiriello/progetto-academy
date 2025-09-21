from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SPARK = "/data/spark/bin/spark-submit"
PYTHON = "/home/dataeng/airflow_venv/bin/python"  # python del venv

with DAG(
    dag_id="pipeline_bronze_silver_gold_local",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["local","spark"],
) as dag:

    bronze = BashOperator(
        task_id="bronze",
        bash_command=f'{SPARK} --master local[*] /home/dataeng/pipeline/bronze.py',
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=f'{SPARK} --master local[*] /home/dataeng/pipeline/silver.py',
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=f'{SPARK} --master local[*] /home/dataeng/pipeline/gold.py',
    )

    ml_sentiment = BashOperator(
        task_id="ml_sentiment",
        bash_command=f'{SPARK} --master local[*] /home/dataeng/pipeline/ml_sentiment.py',
    )

    report = BashOperator(
        task_id="report",
        bash_command=f'{PYTHON} /home/dataeng/pipeline/report.py',
    )

    bronze >> silver >> gold >> ml_sentiment >> report
