from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
METADATA_TABLE = "etl_metadata"

# =====================
# HELPER
# =====================

def get_last_loaded_date(hook):
    hook.run(f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};

        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{METADATA_TABLE} (
            table_name TEXT PRIMARY KEY,
            last_loaded_date DATE
        );
    """)

    result = hook.get_first(f"""
        SELECT last_loaded_date
        FROM {SCHEMA_NAME}.{METADATA_TABLE}
        WHERE table_name = %s
    """, parameters=(TABLE_NAME,))

    return result[0] if result else None

# =====================
# TASK 1: EXTRACT + TRANSFORM
# =====================

def extract_transform(**context):
    df = pd.read_csv(CSV_INPUT_PATH)

    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    df["amount_with_tax"] = df["amount"] * 1.19

    df.to_csv(CSV_OUTPUT_PATH, index=False)
    return CSV_OUTPUT_PATH

# =====================
# TASK 2: LOAD (INCREMENTAL)
# =====================

def load_incremental(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    csv_path = context["ti"].xcom_pull(task_ids="extract_transform")
    df = pd.read_csv(csv_path)

    # 🔥 CRITICAL FIX
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date

    last_date = get_last_loaded_date(hook)

    if last_date:
        df = df[df["order_date"] > last_date]

    if df.empty:
        print("No new data to load.")
        return

    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            order_id INT PRIMARY KEY,
            customer TEXT,
            amount FLOAT,
            amount_with_tax FLOAT,
            order_date DATE
        );
    """)

    for _, r in df.iterrows():
        hook.run(
            f"""
            INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
            (order_id, customer, amount, amount_with_tax, order_date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            """,
            parameters=(
                int(r["order_id"]),
                r["customer"],
                float(r["amount"]),
                float(r["amount_with_tax"]),
                r["order_date"],
            ),
        )

    hook.run(
        f"""
        INSERT INTO {SCHEMA_NAME}.{METADATA_TABLE}
        (table_name, last_loaded_date)
        VALUES (%s, %s)
        ON CONFLICT (table_name)
        DO UPDATE SET last_loaded_date = EXCLUDED.last_loaded_date
        """,
        parameters=(TABLE_NAME, df["order_date"].max()),
    )

    print(f"Loaded {len(df)} new rows.")

# =====================
# TASK 3: DATA QUALITY
# =====================

def data_quality_check():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    row_count = hook.get_first(
        f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{TABLE_NAME}"
    )[0]

    null_count = hook.get_first(
        f"""
        SELECT COUNT(*) 
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE order_id IS NULL
        """
    )[0]

    if row_count == 0:
        raise ValueError("❌ No data loaded!")

    if null_count > 0:
        raise ValueError("❌ NULL order_id found!")

    print("✅ Data quality checks passed.")

# =====================
# DAG
# =====================

with DAG(
    dag_id="csv_to_postgres_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "incremental", "postgres", "dbt"],
) as dag:

    extract = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load_incremental",
        python_callable=load_incremental,
        provide_context=True,
    )

    quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir /home/ubuntu/dbt/airflow_dbt",
    )

    extract >> load >> quality >> dbt_run
