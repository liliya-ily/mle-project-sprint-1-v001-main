# extract_merge_flats_buildings.py

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine
from yaml import safe_load


from messages import send_telegram_success_message, send_telegram_failure_message


@dag(
    schedule='@once',
    start_date=pendulum.datetime(2025, 3, 11, tz="UTC"),
    catchup=False,
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    tags=["ETL"]
)
def create_raw_table():
    @task()
    def extract_and_merge():
        hook = PostgresHook("destination_db")
        conn = hook.get_conn()
        sql = """
            SELECT
                fl.id AS flat_id,
                b.id AS building_id,
                b.build_year,
                b.building_type_int,
                b.latitude,
                b.longitude,
                b.ceiling_height,
                b.flats_count,
                b.floors_total,
                b.has_elevator,
                fl.floor,
                fl.kitchen_area,
                fl.living_area,
                fl.rooms,
                fl.is_apartment,
                fl.studio,
                fl.total_area,
                fl.price
            FROM flats AS fl
            LEFT JOIN buildings AS b ON fl.building_id = b.id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def save_raw_table(data: pd.DataFrame, cfg:dict):
        engine = PostgresHook("destination_db").get_sqlalchemy_engine()
        data.to_sql(cfg["name_raw_table"], engine, if_exists="replace", index=False)

    with open('dags/constants.yaml') as f:
        cfg = safe_load(f)
    
    data = extract_and_merge()
    save_raw_table(data, cfg)

create_raw_table()
