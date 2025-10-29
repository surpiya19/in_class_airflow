from __future__ import annotations
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import task
from psycopg2 import Error as DatabaseError
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import shutil
from faker import Faker

OUTPUT_DIR = "/opt/airflow/data"
TARGET_TABLE = "employees"

default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}  # params for task

with DAG(
    dag_id="pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@once",  # "00 22 * * *",
    catchup=False,
) as dag:

    @task()
    def fetch_persons(output_dir: str = OUTPUT_DIR, quantity=100) -> None:
        fake = Faker()
        data = []
        for _ in range(quantity):
            data.append(
                {
                    "firstname": fake.first_name(),
                    "lastname": fake.last_name(),
                    "email": fake.free_email(),
                    "phone": fake.phone_number(),
                    "address": fake.street_address(),
                    "city": fake.city(),
                    "country": fake.country(),
                }
            )

        filepath = os.path.join(output_dir, "persons.csv")

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        print(f"Data saved to {filepath}")

        return filepath  # type: ignore

    @task()
    def fetch_companies(output_dir: str = OUTPUT_DIR, quantity=100) -> str:
        fake = Faker()
        data = []
        for _ in range(quantity):
            data.append(
                {
                    "name": fake.company(),
                    "email": f"info@{fake.domain_name()}",
                    "phone": fake.phone_number(),
                    "country": fake.country(),
                    "website": fake.url(),
                    "industry": fake.bs().split()[0].capitalize(),
                    "catch_phrase": fake.catch_phrase(),
                    "employees_count": fake.random_int(min=10, max=5000),
                    "founded_year": fake.year(),
                }
            )

        filepath = os.path.join(output_dir, "companies.csv")
        os.makedirs(output_dir, exist_ok=True)

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        print(f"Companies data saved to {filepath}")
        return filepath

    persons_file = fetch_persons()
    companies_file = fetch_companies()
