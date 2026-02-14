from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime

with DAG(
    dag_id='run_my_test_notebook',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # รันแบบ Manual
    catchup=False
) as dag:

    execute_notebook = PapermillOperator(
        task_id="run_notebook_task",
        input_nb="/usr/local/airflow/include/test_notebook.ipynb",
        output_nb="/usr/local/airflow/include/out-{{ ds }}.ipynb",
        kernel_name="python3",  # เพิ่มบรรทัดนี้ครับ
        parameters={
            "input_name": "Gemini User",
            "iteration": 99
        },
    )
