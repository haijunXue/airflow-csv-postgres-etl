from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

# =====================
# CONFIG
# =====================

CSV_INPUT_PATH = "/home/ubuntu/airflow/data/sales.csv"
CSV_OUTPUT_PATH = "/home/ubuntu/airflow/data/sales_transformed.csv"

POSTGRES_CONN_ID = "postgres_dw"
SCHEMA_NAME = "airflow_schema"
TABLE_NAME = "sales"

# =====================
# HELPER
# =====================

def get_postgres_hook():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

# =====================
# TASK 1: EXTRACT + TRANSFORM
# =====================

def extract_transform():
    df = pd.read_csv(CSV_INPUT_PATH)

    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    df["amount_with_tax"] = df["amount"] * 1.19

    df.to_csv(CSV_OUTPUT_PATH, index=False)

    return CSV_OUTPUT_PATH  # XCom

# =====================
# TASK 2: LOAD TO POSTGRES
# =====================

def load_to_postgres(ti):
    csv_path = ti.xcom_pull(task_ids="extract_transform")

    if not csv_path:
        print("No data received from XCom.")
        return

    df = pd.read_csv(csv_path)

    hook = get_postgres_hook()
    conn = hook.get_conn()

    with conn.cursor() as cur:
        # Schema
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")

        # Table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
                order_id INT PRIMARY KEY,
                customer TEXT,
                amount FLOAT,
                amount_with_tax FLOAT,
                order_date DATE
            )
        """)

        # Insert
        for _, r in df.iterrows():
            cur.execute(f"""
                INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
                    order_id, customer, amount, amount_with_tax, order_date
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
            """, (
                int(r["order_id"]),
                r["customer"],
                float(r["amount"]),
                float(r["amount_with_tax"]),
                r["order_date"]
            ))

    conn.commit()
    conn.close()

# =====================
# DAG
# =====================

with DAG(
    dag_id="csv_to_postgres_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "csv", "postgres"]
) as dag:

    extract_transform_task = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    extract_transform_task >> load_task
