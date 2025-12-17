from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Your connection ID
CONN_ID = 'snowflake_conn'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='market_ml_forecasting',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule_interval='@daily',
    tags=['ml', 'snowflake', 'final_project']
) as dag:

    # 1. Prepare Data
    prepare_data = SnowflakeOperator(
        task_id='prepare_training_data',
        snowflake_conn_id=CONN_ID,
        sql="""
            CREATE OR REPLACE TABLE ANALYTICS.market_training_data AS
            SELECT 
                symbol,
                TO_TIMESTAMP_NTZ(date) as date,
                close
            FROM ANALYTICS.stg_market_prices
            WHERE close IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY date) = 1;
        """
    )

    # 2. Train Model
    train_model = SnowflakeOperator(
        task_id='train_forecast_model',
        snowflake_conn_id=CONN_ID,
        autocommit=True,
        sql="""
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST ANALYTICS.market_price_forecaster(
                INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'ANALYTICS.market_training_data'),
                SERIES_COLNAME => 'symbol',
                TIMESTAMP_COLNAME => 'date',
                TARGET_COLNAME => 'close'
            );
        """
    )

    # 3. Generate Forecast
    # Added split_statements=False so Airflow doesn't break the BEGIN/END block
    generate_forecast = SnowflakeOperator(
        task_id='generate_forecast',
        snowflake_conn_id=CONN_ID,
        autocommit=True,
        split_statements=False,  # <--- CRITICAL FIX
        sql="""
            BEGIN
                CALL ANALYTICS.market_price_forecaster!FORECAST(FORECASTING_PERIODS => 14);
                
                CREATE OR REPLACE TABLE ANALYTICS.market_forecast_results AS
                SELECT 
                    series as symbol,
                    ts as forecast_date,
                    forecast as predicted_close
                FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            END;
        """
    )

    prepare_data >> train_model >> generate_forecast