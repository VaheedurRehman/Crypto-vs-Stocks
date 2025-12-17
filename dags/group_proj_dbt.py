from pendulum import datetime
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

DBT_PROJECT_DIR = "/opt/airflow/dbt"

try:
    conn = BaseHook.get_connection('snowflake_conn')
except Exception as e:
    raise AirflowException(f"Failed to get Snowflake connection 'snowflake_conn': {str(e)}")

try:
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
        try:
            dbt_run = BashOperator(
                task_id="dbt_run",
                bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
            )
        except Exception as e:
            raise AirflowException(f"Failed to create dbt_run task: {str(e)}")
        
        try:
            dbt_test = BashOperator(
                task_id="dbt_test",
                bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
            )
        except Exception as e:
            raise AirflowException(f"Failed to create dbt_test task: {str(e)}")
        
        try:
            dbt_snapshot = BashOperator(
                task_id="dbt_snapshot",
                bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
            )
        except Exception as e:
            raise AirflowException(f"Failed to create dbt_snapshot task: {str(e)}")
        
        try:
            dbt_run >> dbt_test >> dbt_snapshot
        except Exception as e:
            raise AirflowException(f"Failed to set task dependencies: {str(e)}")
            
except Exception as e:
    raise AirflowException(f"Failed to create DAG 'group_proj_dbt': {str(e)}")
