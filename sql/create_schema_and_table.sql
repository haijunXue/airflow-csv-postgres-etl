CREATE SCHEMA IF NOT EXISTS airflow_schema;

CREATE TABLE IF NOT EXISTS airflow_schema.sales (
    order_id INT PRIMARY KEY,
    customer TEXT,
    amount FLOAT,
    amount_with_tax FLOAT,
    order_date DATE
);
