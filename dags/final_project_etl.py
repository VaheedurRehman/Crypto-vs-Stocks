from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import yfinance as yf
import sys

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract_and_load():
    """Extract data from yfinance and load directly to Snowflake"""
    
    # Define symbols
    symbols_config = [
        ('SPY', 'stock'),
        ('QQQ', 'stock'),
        ('BTC-USD', 'crypto'),
        ('ETH-USD', 'crypto')
    ]
    
    # Extract all data
    all_records = []
    for symbol, asset_type in symbols_config:
        print(f"Extracting data for {symbol}...")
        sys.stdout.flush()
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="2y")
        df['symbol'] = symbol
        df['asset_type'] = asset_type
        df.reset_index(inplace=True)
        
        for _, row in df.iterrows():
            all_records.append((
                row['symbol'],
                row['Date'].strftime('%Y-%m-%d'),
                float(row['Open']),
                float(row['High']),
                float(row['Low']),
                float(row['Close']),
                int(row['Volume']),
                row['asset_type']
            ))
    
    print(f"Total records extracted: {len(all_records)}")
    sys.stdout.flush()
    
    # Load to Snowflake
    cur = return_snowflake_conn()
    target_table = "USER_DB_QUAIL.raw.market_prices"
    
    print("Setting Snowflake context...")
    sys.stdout.flush()
    cur.execute("USE WAREHOUSE QUAIL_QUERY_WH")
    cur.execute("USE DATABASE USER_DB_QUAIL")
    cur.execute("USE SCHEMA raw")
    print("Context set successfully")
    sys.stdout.flush()
    
    # Drop and recreate table for clean load
    print("Dropping existing table...")
    sys.stdout.flush()
    cur.execute(f"DROP TABLE IF EXISTS {target_table}")
    
    print("Creating table...")
    sys.stdout.flush()
    cur.execute(f"""
        CREATE TABLE {target_table} (
            symbol VARCHAR(20),
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            asset_type VARCHAR(10),
            PRIMARY KEY (symbol, date)
        )
    """)
    print("Table created")
    sys.stdout.flush()
    
    # Batch insert using executemany (much faster!)
    print("Inserting records in batch...")
    sys.stdout.flush()
    
    insert_sql = f"""
        INSERT INTO {target_table} (symbol, date, open, high, low, close, volume, asset_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cur.executemany(insert_sql, all_records)
    
    print(f"Batch insert complete")
    sys.stdout.flush()
    
    # Verify the data
    cur.execute(f"SELECT COUNT(*) FROM {target_table}")
    count = cur.fetchone()[0]
    print(f"Verification: Table now has {count} records")
    sys.stdout.flush()
    
    return f"Loaded {count} records"

# Define the DAG
with DAG(
    dag_id='final_project_etl',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule_interval='@daily',
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    tags=['etl', 'yfinance', 'project']
) as dag:
    
    extract_and_load()