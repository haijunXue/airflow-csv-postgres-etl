# 📊 Cloud-Based ETL Pipeline with Airflow, PostgreSQL & dbt (AWS)

## 🚀 Project Overview
This project implements a **production-style end-to-end ETL pipeline deployed on an AWS EC2 instance** using **Apache Airflow**, **PostgreSQL**, and **dbt**.

Sales data is extracted from CSV files, transformed using Python (pandas), loaded **incrementally** into a PostgreSQL data warehouse, validated with data quality checks, and transformed into analytics-ready tables using dbt.

The entire workflow is orchestrated and monitored with Apache Airflow.

---

## ☁️ Cloud Deployment (AWS)

The pipeline runs on a **Linux-based AWS EC2 instance**, simulating a real-world production data platform.

**Infrastructure**
- AWS EC2 (Linux)
- Apache Airflow
- PostgreSQL (Data Warehouse)
- dbt (Analytics Engineering)

**Responsibilities**
- EC2 environment setup
- Python virtual environment management
- Airflow DAG orchestration and scheduling
- PostgreSQL connection and schema management
- End-to-end pipeline monitoring

---

## 🏗 Architecture

CSV Files
↓
Apache Airflow (ETL Orchestration)
↓
PostgreSQL (Raw Data Layer)
↓
dbt (Staging & Fact Models)

---

## 🔧 Tech Stack

- Apache Airflow
- Python (pandas)
- PostgreSQL
- dbt
- SQL
- AWS EC2
- Linux & Bash

---

## ✨ Key Features

- Incremental data loading using metadata tracking
- Idempotent inserts (`ON CONFLICT DO NOTHING`)
- Data quality checks (empty tables, NULL primary keys)
- dbt staging and fact models
- Fully automated DAG execution

---

## 📁 Project Structure

├── dags/
│ └── csv_to_postgres_etl.py
├── data/
│ └── sales.csv
├── dbt/
│ └── airflow_dbt/
│ ├── dbt_project.yml
│ └── models/
│ ├── staging/
│ │ └── stg_sales.sql
│ └── marts/
│ └── fct_sales.sql
├── .gitignore
└── README.md


---

## 🔄 ETL Workflow

### 1️⃣ Extract & Transform
- Reads sales data from CSV
- Converts date fields
- Calculates `amount_with_tax`

### 2️⃣ Incremental Load
- Loads only new records based on `last_loaded_date`
- Stores ETL metadata in a dedicated table

### 3️⃣ Data Quality Checks
- Verifies that data exists
- Ensures primary key integrity

### 4️⃣ dbt Transformations
- Builds staging models
- Creates analytics-ready fact tables

---

## ▶️ How to Run

### 1. Install dependencies
```bash
pip install apache-airflow pandas psycopg2-binary dbt-postgres
2. Start Airflow
airflow db init
airflow scheduler
airflow webserver
3. Trigger the DAG
airflow dags trigger csv_to_postgres_etl
📌 Future Improvements

Bulk inserts for large datasets

dbt tests and snapshots

Dockerized deployment

CI/CD for dbt models

Cloud-native services (S3, RDS)

Skills:
Airflow · AWS · dbt · SQL · Python · Data Engineering
