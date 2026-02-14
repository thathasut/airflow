from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
# หรือถ้ายังอยากใช้ PostgresOperator โดยตรง (ซึ่งเรียกผ่าน Common SQL เช่นกัน)
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='notebook_to_postgres',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    # 1. รัน Notebook ประมวลผล
    run_nb = PapermillOperator(
        task_id="run_analysis",
        input_nb="/usr/local/airflow/include/test_notebook.ipynb",
        output_nb="/usr/local/airflow/include/out-{{ ds }}.ipynb",
        kernel_name="python3",
        # ส่งค่าตัวแปรจาก Airflow เข้าไปใน Notebook
        parameters={
            "execution_date": "{{ ds }}",      # ส่งวันที่รัน (YYYY-MM-DD)
            "run_id": "{{ run_id }}",          # ส่ง ID ของการรันครั้งนี้
            "threshold": 100                   # หรือจะส่งค่าคงที่เข้าไปก็ได้
        }
    )

    # 2. บันทึกสถานะ
    log_to_db = PostgresOperator(
        task_id="log_success_to_db",
        postgres_conn_id="my_postgres_db",
        sql="""
            -- เพิ่มบรรทัดนี้เพื่อสร้างตารางถ้ายังไม่มี
            CREATE TABLE IF NOT EXISTS run_logs (
                run_date DATE,
                status VARCHAR(50)
            );

            INSERT INTO run_logs (run_date, status)
            VALUES ('{{ ds }}', 'Success');
        """
    )

    # เชื่อมต่อ Task (ตัวแปรต้องสะกดเหมือนเป๊ะๆ)
    run_nb >> log_to_db
