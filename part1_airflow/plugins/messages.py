from airflow.providers.telegram.hooks.telegram import TelegramHook
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv()

def send_telegram_success_message(context):
    chat_id = os.environ.get("CHAT_ID")
    token = os.environ.get('TG_TOKEN')
    hook = TelegramHook(token=f"{token}", chat_id=f"{chat_id}")
    dag = context["dag"].dag_id
    run_id = context["run_id"]
    message = f"Исполнение DAG {dag} с id={run_id} прошло успешно!"
    hook.send_message({"chat_id": f"{chat_id}", "text": message})



def send_telegram_failure_message(context):
    chat_id = os.environ.get("CHAT_ID")
    token = os.environ.get('TG_TOKEN')
    hook = TelegramHook(token=f"{token}", chat_id=f"{chat_id}")
    dag = context["dag"].dag_id
    run_id = context["run_id"]
    task_instance_key_str = context["task_instance_key_str"]

    message = f"Исполнение DAG {dag} с id={run_id} прошло неуспешно!\n. Задача: {task_instance_key_str}"
    hook.send_message({"chat_id": f"{chat_id}", "text": message})
