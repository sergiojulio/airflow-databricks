from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

DATABRICKS_CONN_ID = 'databricks_default'

notebook_task_config = {
    "run_name": "My Serverless Notebook Run",
    "tasks": [
        {
            "task_key": "run_notebook",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/sergiojulio/notebook"
            },
            "compute": {
                "compute_type": "SERVERLESS"  # <-- THIS IS THE FIX
            }
        }
    ]
}

with DAG(
    dag_id="datbricks",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    run_notebook = DatabricksSubmitRunOperator(
        task_id="run_my_databricks_notebook",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=notebook_task_config,
    )

    run_notebook
