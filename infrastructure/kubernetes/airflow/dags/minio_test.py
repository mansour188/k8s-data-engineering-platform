from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def test_minio_integration():
    print("======================================")
    print("Airflow -> MinIO integration works")
    print("DAG was loaded from MinIO")
    print("======================================")


with DAG(
    dag_id="minio_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "minio"],
) as dag:

    test = PythonOperator(
        task_id="test_minio_integration",
        python_callable=test_minio_integration,
    )
