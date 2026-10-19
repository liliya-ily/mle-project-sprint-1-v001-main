# Оценка стоимости квартир: пайплайны подготовки данных и обучения модели

Учебный проект курса повышения квалификации «Инженер машинного обучения» (Яндекс Практикум, спринт 1).

## Задача

Яндекс Недвижимость — маркетплейс аренды и покупки жилой и коммерческой недвижимости. Часто сделки срываются из-за расхождения оценок стоимости квартиры у продавца и покупателя. Гипотеза продуктовой команды: объективная внешняя оценка цены увеличит число сделок.

Цель проекта — построить MVP алгоритма, который оценивает рыночную стоимость квартиры по её характеристикам (задача регрессии), и оформить весь процесс как воспроизводимые пайплайны:

1. **Сбор данных** — Airflow объединяет таблицы домов и квартир в единый датасет.
2. **Очистка данных** — Airflow обрабатывает дубликаты и выбросы и строит признаки.
3. **Обучение модели** — DVC-пайплайн: выгрузка данных, обучение CatBoost, кросс-валидация, хранение артефактов в S3.

## Данные

Исходные данные находились в персональной PostgreSQL-базе на учебном сервере Яндекс Практикума. Две таблицы связаны один-ко-многим: `buildings.id = flats.building_id`.

`buildings`: `id`, `build_year`, `building_type_int`, `latitude`, `longitude`, `ceiling_height`, `flats_count`, `floors_total`, `has_elevator`.

`flats`: `id`, `building_id`, `floor`, `kitchen_area`, `living_area`, `rooms`, `is_apartment`, `studio`, `total_area`, `price`.

## Что сделано

**Часть 1: ETL в Airflow (`part1_airflow`)**
- Объединение таблиц в `features_raw`.
- Удаление дубликатов, выбросов по IQR и по цене за квадратный метр.
- Группировка признаков (`floors_total`, `floor`, `rooms`, `build_year`) в категории low / med / hig.
- Запись очищенного датасета в БД; уведомления об успехе и ошибке — в Telegram.
- Исследование данных и обоснование очистки — в ноутбуке `notebooks/clear_data.ipynb`.

**Часть 2: DVC-пайплайн (`part2_dvc`)**
- Стадии: `get_data` → `fit_model` → `evaluate_model`.
- Модель: CatBoostRegressor в sklearn `Pipeline` с `ColumnTransformer`.
- Метрики кросс-валидации (5 фолдов): R², RMSE, максимальная ошибка.
- Модель и результаты версионируются DVC и хранятся в S3.

## Структура проекта

**DAG-и Airflow**

| Файл | DAG |
|---|---|
| `part1_airflow/dags/create_raw_table.py` | `create_raw_table` |
| `part1_airflow/dags/transform_features.py` | `transform_features` |

Параметры DAG-ов: `part1_airflow/dags/constants.yaml`. Уведомления в Telegram: `part1_airflow/plugins/messages.py`.

**DVC-пайплайн**

| Стадия | Скрипт |
|---|---|
| `get_data` | `part2_dvc/scripts/data.py` |
| `fit_model` | `part2_dvc/scripts/fit.py` |
| `evaluate_model` | `part2_dvc/scripts/evaluate.py` |

Конфигурация: `part2_dvc/dvc.yaml`, `part2_dvc/params.yaml`, `part2_dvc/dvc.lock`. Общие функции препроцессинга: `part2_dvc/scripts/func.py`. Ноутбук с прототипом модели: `part2_dvc/notebooks/fitting.ipynb`.

## Установка и запуск

См. [INSTALL.md](INSTALL.md).
