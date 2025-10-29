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
    dag_id="pipeline",  # type: ignore
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

    @task()
    def merge_csvs(
        persons_path: str, companies_path: str, output_dir: str = OUTPUT_DIR
    ) -> str:
        merged_path = os.path.join(output_dir, "merged_data.csv")

        with open(persons_path, newline="", encoding="utf-8") as f1, open(
            companies_path, newline="", encoding="utf-8"
        ) as f2:

            persons_reader = list(csv.DictReader(f1))
            companies_reader = list(csv.DictReader(f2))

        merged_data = []
        for i in range(min(len(persons_reader), len(companies_reader))):
            person = persons_reader[i]
            company = companies_reader[i]
            merged_data.append(
                {
                    "firstname": person["firstname"],
                    "lastname": person["lastname"],
                    "email": person["email"],
                    "company_name": company["name"],
                    "company_email": company["email"],
                }
            )

        with open(merged_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=merged_data[0].keys())
            writer.writeheader()
            writer.writerows(merged_data)

        print(f"Merged CSV saved to {merged_path}")
        return merged_path

    persons_file = fetch_persons()
    companies_file = fetch_companies()
    merged_path = merge_csvs(persons_file, companies_file)  # type: ignore
