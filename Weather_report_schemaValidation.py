from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(
    dag_id="Pl_report_schemaValidation_dag",
    schedule_interval="40 11 * * *",
    start_date=datetime(2025, 8, 9),
    catchup=False
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    },
    tags=["reporting", "Schema Validation"],
    default_view="graph",
    description="Counting the number of file and Schema validation"
)
def report_schemaValidation_dag():
    @task()
     