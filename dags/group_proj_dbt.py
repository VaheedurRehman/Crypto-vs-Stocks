from pendulum import datetime
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

DBT_PROJECT_DIR = "/opt/airflow/dbt"
conn = BaseHook.get_connection('snowflake_conn')

with DAG(
    "group_proj_dbt",
    start_date=datetime(2025, 11, 1),
    description="Run dbt models to transform market data",
    schedule_interval=None, # Manual trigger for now
    catchup=False,
    default_args={
        "owner": "calvin",
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": "ANALYTICS", 
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run >> dbt_test >> dbt_snapshot