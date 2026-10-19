# clean_transform_features.py

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
import pandas as pd
import numpy as np
from yaml import safe_load
from datetime import timedelta


from messages import send_telegram_success_message, send_telegram_failure_message


def remove_duplicates(data, not_use_col, keep):
    feature_cols = [i for i in data.columns if i not in not_use_col]
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=keep)
    data = data[~is_duplicated_features]
    return data 

def clean_iqr(data, list_col):
    threshold = 1.5
    potential_outliers_id = list()

    for col in list_col:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        margin = threshold * IQR
        lower = Q1 - margin
        upper = Q3 + margin
        potential_outliers_id.extend(
            data[~data[col].between(lower, upper)]["flat_id"].tolist()
        )
        print(
            f"По признаку {col} границы lower {round(lower, 2)} upper {round(upper, 2)}, было удалено: {data[~data[col].between(lower, upper)].shape[0]} flat_id"
        )

    return data[~data["flat_id"].isin(set(potential_outliers_id))]


def label_discrete_feature(series, bins=None, labels=None, iqr_multiplier=1.5):
    """
    Разбивает дискретную числовую переменную на категории:
    по умолчанию: до Q3, от Q3 до Q3+1.5*IQR, выше Q3+1.5*IQR.
    
    Если bins переданы — используется кастомное разбиение.
    """
    if bins is None:
        q3 = series.quantile(0.75)
        q1 = series.quantile(0.25)
        iqr = q3 - q1
        upper = q3 + iqr_multiplier * iqr
        bins = [float('-inf'), q3, upper, float('inf')]
    
    if labels is None:
        labels = ['low', 'med', 'hig']
    
    return pd.cut(series, bins=bins, labels=labels, right=True, include_lowest=True)

def bin_features(data, cfg):
    for col in cfg['binning_cols']:
        data[col + '_bin'] = label_discrete_feature(data[col])

    # Кастомное бинингование для build_year
    build_year_bins = [float('-inf'), 1955, 1991, float('inf')]
    build_year_labels = ['low', 'med', 'hig']
    data['build_year_bin'] = label_discrete_feature(
        data['build_year'], bins=build_year_bins, labels=build_year_labels
    )

    return data

def clean_target(data, min_value, max_value):
    
    if sum(data['total_area'] == 0) > 0:
        raise ValueError('total_area не может быть равным нулю')
    data['price_by_total'] = data['price'] / data['total_area']

    data['target_outliers'] = np.where(((data['total_area'] < min_value) & (data['price_by_total'] > max_value)) | (data['price_by_total'] < 2), True, False)
    data = data[~data['target_outliers']]
    data = data.drop(columns=['price_by_total', 'target_outliers'])
    
    return data

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2025, 3, 11, tz="UTC"),
    catchup=False,
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    tags=["ETL"]
)
def transform_features():
    @task()
    def load_raw(cfg:dict):
        hook = PostgresHook("destination_db")
        conn = hook.get_conn()
        data = pd.read_sql(f"SELECT * FROM {cfg['name_raw_table']}", conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame, cfg):
        data = remove_duplicates(data, ['flat_id'], 'first')
        data = remove_duplicates(data, ['flat_id', 'building_id', 'price'], False)

        # -- defind cols
        bool_cols = data.select_dtypes(include=bool).columns

        digital_cols = list(
            set(data.columns)
            - set([cfg['target_col']])
            - set(cfg['id_cols'])
            - set(bool_cols)
            - set(cfg['cat_ordinal_cols'])
            - set(cfg['cat_cols'])
            - set(cfg['digital_notfilter_cols'])
        )
        # -- iqr
        data = clean_iqr(data, digital_cols)
        # -- bins
        data = bin_features(data, cfg)
        # -- target
        data = clean_target(data, 100, 1_000_000)
        return data.drop(columns=['studio', 'floors_total', 'floor', 'rooms', 'build_year'])

    @task()
    def create_table(cfg:dict):
        from sqlalchemy import create_engine, Table, Column, Boolean, String, Float, Numeric, Integer, MetaData, UniqueConstraint, inspect
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        metadata = MetaData()

        clean_url = str(engine.url).split("?")[0]  # Обрезаем всё после "?"
        clean_engine = create_engine(clean_url)  # Создаем новый engine 

        metadata = MetaData()
        flat_table = Table(cfg['name_feature_table'], metadata,
            Column('id', Integer, primary_key=True, autoincrement=True), 
            # Данные здания
            Column('building_id', Integer),  # ID здания (buildings.id)
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            # Данные квартиры
            Column('flat_id', Integer),  # ID квартиры (flats.id)
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', Boolean),
            Column('total_area', Float),
            Column('floors_total_bin', String),
            Column('floor_bin', String),
            Column('rooms_bin', String),
            Column('build_year_bin', String),
            Column('price', Numeric),
            UniqueConstraint('flat_id', name='unique_flat_id')
        )

        with clean_engine.connect() as conn:
            # Удаляем таблицу, если она существует
            if inspect(clean_engine).has_table(flat_table.name):
                flat_table.drop(conn)
                print(f"Таблица {flat_table.name} была удалена.")

            # Создаем новую таблицу
            metadata.create_all(conn)
            print(f"Таблица {flat_table.name} была создана.")
    
        
    
    @task()
    def load(data: pd.DataFrame, cfg:dict):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table=cfg['name_feature_table'],
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
        )    
        
        
    with open('dags/constants.yaml') as f:
        cfg = safe_load(f)

    data = load_raw(cfg)

    transformed = transform(data, cfg)
    create_table(cfg)
    load(transformed, cfg)

transform_features()
