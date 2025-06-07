import httpx
from typing import Dict
from core.config import get_settings

settings = get_settings()

AIRFLOW_BASE_URL = settings.AIRFLOW__WEBSERVER__BASE_URL or "http://airflow-webserver:8080"
AIRFLOW_DAG_ID = "idp_blueprint_runner"  # your DAG id

async def trigger_dag_run(conf: Dict):
    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns"

    payload = {
        "conf": conf
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload, auth=("airflow", "airflow"))  # replace with actual creds
        response.raise_for_status()
        return response.json()
