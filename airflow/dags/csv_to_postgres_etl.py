from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

# =====================
# CONFIG
# =====================

CSV_INPUT_PATH = "/home/ubuntu/airflow/data/sales.csv"
CSV_OUTPUT_PATH = "/home/ubuntu/airflow/data/sales_transformed.csv"

DB_CONFIG = {
    "host": "localhost",
    "database": "airflow_dw",
    "user": "airflow_user",
    "password": "airflow123",
    "port": 5432
}

SCHEMA_NAME = "airflow_schema"  # explizit definiert
TABLE_NAME = "sales"            # explizit definiert

# =====================
# TASK 1: EXTRACT + TRANSFORM
# =====================

def extract_transform():
    # CSV lesen
    df = pd.read_csv(CSV_INPUT_PATH)

    # Transformation
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    df["amount_with_tax"] = df["amount"] * 1.19

    # Transformierte Daten speichern
    df.to_csv(CSV_OUTPUT_PATH, index=False)

    return CSV_OUTPUT_PATH  # für XCom

# =====================
# TASK 2: LOAD TO POSTGRES
# =====================

def load_to_postgres(ti):
    csv_path = ti.xcom_pull(task_ids="extract_transform")
    df = pd.read_csv(csv_path)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Schema erstellen, falls nicht vorhanden
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME} AUTHORIZATION {DB_CONFIG['user']}")

            # Tabelle erstellen
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
                    order_id INT PRIMARY KEY,
                    customer TEXT,
                    amount FLOAT,
                    amount_with_tax FLOAT,
                    order_date DATE
                )
            """)

            # Daten einfügen
            inserted_rows = 0
            for _, r in df.iterrows():
                cur.execute(f"""
                    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
                        order_id,
                        customer,
                        amount,
                        amount_with_tax,
                        order_date
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO NOTHING
                """, (
                    int(r["order_id"]),
                    r["customer"],
                    float(r["amount"]),
                    float(r["amount_with_tax"]),
                    r["order_date"]
                ))
                inserted_rows += cur.rowcount

            conn.commit()
            print(f"Inserted {inserted_rows} rows into {SCHEMA_NAME}.{TABLE_NAME}")

    finally:
        conn.close()

# =====================
# DAG DEFINITION
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
