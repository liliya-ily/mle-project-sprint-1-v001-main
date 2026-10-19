# Установка и запуск

Проект состоит из двух частей:

- `part1_airflow` — ETL в Airflow (Docker): собирает признаки квартир в PostgreSQL.
- `part2_dvc` — пайплайн DVC: выгрузка данных, обучение модели CatBoost, кросс-валидация.

## Требования

- Python 3.10
- Git
- Docker и Docker Compose (для части 1)
- Доступ к PostgreSQL и S3-совместимому хранилищу (для DVC)
- Рекомендуется Linux или WSL: `requirements.txt` содержит пакеты, которые не устанавливаются на Windows

## Конфигурация (`.env`)

Файл `.env` нужен в двух местах: в корне проекта (скрипты части 2) и в `part1_airflow/` (docker compose)

```env
# подключение к PostgreSQL
DB_DESTINATION_HOST=<хост>
DB_DESTINATION_PORT=<порт>
DB_DESTINATION_NAME=<имя базы>
DB_DESTINATION_USER=<пользователь>
DB_DESTINATION_PASSWORD=<пароль>

# только part1_airflow/.env
AIRFLOW_UID=50000
AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
TG_TOKEN=<токен Telegram-бота>   # уведомления о запусках DAG
CHAT_ID=<id чата>
```

## Часть 1. Airflow

```bash
cd part1_airflow
docker compose up airflow-init
docker compose up -d --build
```

Веб-интерфейс: http://localhost:8080 (логин и пароль по умолчанию `airflow` / `airflow`).

Для DAG-ов нужно соединение Airflow `destination_db` (Admin → Connections, тип Postgres) с параметрами базы из `.env`.

Запустите DAG-и по порядку:

1. `create_raw_table` — объединяет таблицы `flats` и `buildings` в таблицу `features_raw`.
2. `transform_features` — очищает данные, строит бины и записывает таблицу `flats_feature`.

Параметры обработки находятся в `part1_airflow/dags/constants.yaml`.

## Часть 2. DVC и обучение модели

Подготовка окружения:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
git init            # DVC работает внутри git-репозитория
```

Подключение хранилища DVC (S3):

```bash
cd part2_dvc
dvc remote add -d my_storage s3://<имя_бакета>
dvc remote modify my_storage endpointurl <endpoint S3>
dvc remote modify --local my_storage access_key_id <ключ>
dvc remote modify --local my_storage secret_access_key <секрет>
```

Запуск пайплайна:

```bash
dvc repro      # get_data → fit_model → evaluate_model
dvc push       # отправить артефакты в хранилище
```

Результаты:

- `data/initial_data.csv` — выгруженные данные
- `models/fitted_model.pkl` — обученная модель
- `cv_results/cv_res.json` — метрики кросс-валидации

Параметры обучения и валидации находятся в `part2_dvc/params.yaml`.

Скрипты `get_data` подключаются к базе по переменным из `.env` в корне проекта и читают таблицу `flats_info`.

## Ноутбуки

- `part1_airflow/notebooks/clear_data.ipynb` — исследование и очистка данных
- `part2_dvc/notebooks/fitting.ipynb` — прототип модели

Откройте ноутбук в Jupyter или VS Code с ядром из активированного окружения (подключение к базе берётся из `.env`).
