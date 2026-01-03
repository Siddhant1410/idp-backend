from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv() 

# === Secrets === #
SECRET_KEY = os.getenv("SECRET_KEY")  # Must be exactly 32 bytes
MONGO_URI = os.getenv("MONGO_URI")
INGESTION_URL = os.getenv("UI_PORTAL_INGESTION_URL") #Ingestion URL of UI portal

# === DAG Trigger CONFIG === #
AIRFLOW_API_URL = "http://airflow-airflow-apiserver-1:8080/api/v2"  # or localhost in local mode
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD")
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"

if LOCAL_MODE:
    AIRFLOW_API_URL = "http://localhost:8080/api/v2"

# === CONFIG === #
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
MONGO_DB_NAME = "idp"
MONGO_COLLECTION = "LogEntry"
mongo_client = MongoClient(MONGO_URI)
mongo_collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION]

# ---------------- CONFIG ---------------- #

NODE_TO_DAG_MAP = {
    "Ingestion": "ingest_documents_dag",
    "Classify": "classify_documents_dag",
    "Extract": "extract_documents_dag",
    "Validate": "highlight_extracted_fields_dag",
    "Deliver": "deliver_dag"
}

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 0
}

# --------------------------------------- #

def read_blueprint(**context):
    ti = context["ti"]
    dag_run = context["dag_run"]

    # processInstanceId must be passed when triggering Service DAG
    process_instance_id = dag_run.conf.get("id")
    if not process_instance_id:
        raise AirflowFailException("Missing process_instance_id in dag_run.conf")

    # Same local folder logic
    LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
    process_instance_dir_path = os.path.join(
        LOCAL_DOWNLOAD_DIR, f"process-instance-{process_instance_id}"
    )
    os.makedirs(process_instance_dir_path, exist_ok=True)

    BLUEPRINT_JSON_PATH = os.path.join(process_instance_dir_path, "blueprint.json")

    # -------- MySQL connection (EXACT SAME AS INGESTION DAG) -------- #
    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # 1. Get processesId
        cursor.execute("""
            SELECT processesId 
            FROM ProcessInstances 
            WHERE id = %s
        """, (process_instance_id,))
        instance_data = cursor.fetchone()

        if not instance_data:
            raise ValueError(f"No process instance found with ID {process_instance_id}")

        process_id = instance_data[0]

        # 2. Get processInstanceFolderName
        cursor.execute("""
            SELECT processInstanceFolderName 
            FROM ProcessInstances 
            WHERE id = %s
        """, (process_instance_id,))
        instance_data = cursor.fetchone()

        process_instance_folder = instance_data[0]

        # 3. Get bluePrintId from Processes
        cursor.execute("""
            SELECT bluePrintId 
            FROM Processes 
            WHERE id = %s
        """, (process_id,))
        blueprint_id_row = cursor.fetchone()

        if not blueprint_id_row or not blueprint_id_row[0]:
            raise ValueError(f"No bluePrintId found for process ID {process_id}")

        blueprint_id = blueprint_id_row[0]

        # 4. Get blueprint JSON
        cursor.execute("""
            SELECT bluePrint 
            FROM BluePrint 
            WHERE id = %s
        """, (blueprint_id,))
        blueprint_row = cursor.fetchone()

        if not blueprint_row or not blueprint_row[0]:
            raise ValueError(f"No blueprint found for blueprint ID {blueprint_id}")

        blueprint_json = json.loads(blueprint_row[0])

        # 5. Save blueprint locally (same pattern)
        with open(BLUEPRINT_JSON_PATH, "w") as f:
            json.dump(blueprint_json, f, indent=2)

        print(f"✅ Blueprint saved to {BLUEPRINT_JSON_PATH}")

        # 6. Extract node execution order
        node_sequence = [
            node["nodeName"]
            for node in blueprint_json
            if isinstance(node, dict) and "nodeName" in node
        ]

        # 7. Push to XCom for orchestrator logic
        ti.xcom_push(key="node_sequence", value=node_sequence)
        ti.xcom_push(key="process_instance_folder", value=process_instance_folder)

        print(f"🧬 Blueprint execution sequence: {node_sequence}")

    except Exception as e:
        conn.rollback()
        raise AirflowFailException(str(e))

    finally:
        cursor.close()
        conn.close()

# --------------------------------------- #

with DAG(
    dag_id="idp_service_orchestrator",
    default_args=DEFAULT_ARGS,
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["idp", "service_orchestrator"],
) as dag:

    read_blueprint_task = PythonOperator(
        task_id="read_blueprint",
        python_callable=read_blueprint
    )

    previous_task = read_blueprint_task

    for node_name, dag_id in NODE_TO_DAG_MAP.items():

        trigger = TriggerDagRunOperator(
            task_id=f"run_{node_name.lower()}",
            trigger_dag_id=dag_id,
            conf={
                "id": "{{ dag_run.conf['id'] }}",
                "node_name": node_name,
                "execution_source": "service_orchestrator"
            },
            wait_for_completion=True,     
            poke_interval=60,
            reset_dag_run=True,           # optional but recommended
            trigger_rule="none_failed"
        )

        trigger.skip_when = lambda context, n=node_name: (
            n not in context["ti"].xcom_pull(
                task_ids="read_blueprint",
                key="node_sequence"
            )
        )

        previous_task >> trigger
        previous_task = trigger

