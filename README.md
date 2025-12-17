# Crypto vs Stocks - Market Analysis & ML Forecasting Pipeline

A comprehensive data engineering and machine learning project that extracts, transforms, and analyzes market data for stocks and cryptocurrencies, with built-in ML forecasting capabilities using Snowflake's ML features.

## 📊 Project Overview

This project implements a complete ELT (Extract, Load, Transform) pipeline using modern data engineering tools to compare and forecast market performance between traditional stocks (SPY, QQQ) and cryptocurrencies (BTC-USD, ETH-USD). The pipeline leverages Apache Airflow for orchestration, dbt for transformations, and Snowflake for data warehousing and machine learning.

## 🏗️ Architecture

```
yfinance API → Airflow ETL → Snowflake (Raw Layer)
                                ↓
                        dbt Transformations
                                ↓
                    Snowflake Analytics Layer
                                ↓
                    ML Forecasting (Snowflake ML)
                                ↓
                        Forecast Results
                                ↓
                          Preset Dashboard
```

## 🚀 Features

- **Automated Data Extraction**: Pulls 2 years of historical market data from Yahoo Finance
- **Data Transformation**: Uses dbt for staging, data quality, and analytics transformations
- **ML Forecasting**: Leverages Snowflake's ML capabilities for 14-day price predictions
- **Error Handling**: Comprehensive try-except blocks throughout the pipeline
- **Orchestration**: Fully automated workflows using Apache Airflow
- **Data Quality**: Built-in tests and snapshots for data validation

## 📁 Project Structure

```
Crypto-vs-Stocks/
├── dags/
│   ├── final_project_etl.py          # Main ETL pipeline (yfinance → Snowflake)
│   ├── group_proj_dbt.py             # dbt transformation orchestration
│   └── market_ml_forecasting.py      # ML model training & forecasting
├── dbt/
│   ├── models/
│   │   ├── staging/                  # Staging models
│   │   └── analytics/                # Analytics models
│   ├── snapshots/                    # Historical data snapshots
│   └── dbt_project.yml               # dbt configuration
├── logs/                              # Airflow logs
├── Dockerfile                         # Custom Airflow image
└── docker-compose.yml                 # Docker orchestration
```

## 🔧 Technology Stack

- **Orchestration**: Apache Airflow 2.x
- **Data Warehouse**: Snowflake
- **Transformation**: dbt (Data Build Tool)
- **Data Source**: yfinance (Yahoo Finance API)
- **ML/Forecasting**: Snowflake ML Functions
- **Containerization**: Docker & Docker Compose
- **Language**: Python 3.x

## 📦 Installation & Setup

### Prerequisites

- Docker & Docker Compose
- Snowflake account with warehouse, database, and schema configured
- Python 3.8+

### 1. Clone the Repository

```bash
git clone https://github.com/VaheedurRehman/Crypto-vs-Stocks.git
cd Crypto-vs-Stocks
```

### 2. Configure Snowflake Connection

Set up your Snowflake connection in Airflow:

```python
Connection ID: snowflake_conn
Connection Type: Snowflake
Host: <your-account>.snowflakecomputing.com
Schema: ANALYTICS
Login: <your-username>
Password: <your-password>
Extra: {
    "account": "<your-account>",
    "warehouse": "QUAIL_QUERY_WH",
    "database": "USER_DB_QUAIL",
    "role": "<your-role>"
}
```

### 3. Start Docker Environment

```bash
docker-compose up -d
```

### 4. Access Airflow UI

Navigate to `http://localhost:8080` and log in with default credentials.

## 🎯 Pipeline Workflows

### 1. ETL Pipeline (`final_project_etl`)

**Schedule**: Daily (`@daily`)

**Process**:
1. Extracts 2 years of historical data for SPY, QQQ, BTC-USD, ETH-USD
2. Loads data into `USER_DB_QUAIL.raw.market_prices`
3. Validates data integrity

**Tables Created**:
- `raw.market_prices`: Raw price data with columns (symbol, date, open, high, low, close, volume, asset_type)

### 2. dbt Transformation Pipeline (`group_proj_dbt`)

**Schedule**: Manual trigger

**Process**:
1. **dbt run**: Executes staging and analytics models
2. **dbt test**: Runs data quality tests
3. **dbt snapshot**: Creates historical snapshots

**Models**:
- Staging: `stg_market_prices`
- Analytics: Market aggregations and analysis

### 3. ML Forecasting Pipeline (`market_ml_forecasting`)

**Schedule**: Daily (`@daily`)

**Process**:
1. **Prepare Training Data**: Deduplicates and formats data
2. **Train Model**: Creates Snowflake ML forecast model
3. **Generate Forecast**: Produces 14-day predictions

**Tables Created**:
- `ANALYTICS.market_training_data`: Cleaned training dataset
- `ANALYTICS.market_price_forecaster`: ML model object
- `ANALYTICS.market_forecast_results`: 14-day price forecasts

## 📊 Data Models

### Raw Layer
```sql
raw.market_prices (
    symbol VARCHAR(20),
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    asset_type VARCHAR(10)
)
```

### Analytics Layer
```sql
ANALYTICS.market_training_data (
    symbol VARCHAR(20),
    date TIMESTAMP_NTZ,
    close FLOAT
)

ANALYTICS.market_forecast_results (
    symbol VARCHAR(20),
    forecast_date TIMESTAMP_NTZ,
    predicted_close FLOAT
)
```

## 🔍 Key Features

### Error Handling
All DAGs include comprehensive error handling with:
- Try-except blocks for each task
- Meaningful AirflowException messages
- Detailed logging for debugging
- Automatic retries on transient failures

### Data Quality
- Deduplication logic using `ROW_NUMBER()` window functions
- NULL value filtering
- Primary key constraints
- dbt test suite for validation

### ML Forecasting
- Time-series forecasting using Snowflake ML
- 14-day forward predictions
- Symbol-level forecasting (multi-series support)
- Automated model retraining

## 🎓 Use Cases

- **Market Analysis**: Compare crypto vs stock performance
- **Price Prediction**: Forecast future prices using ML
- **Portfolio Strategy**: Data-driven investment decisions
- **Research**: Historical trend analysis
- **Education**: Learn modern data engineering practices

## 📈 Sample Queries

### View Latest Prices
```sql
SELECT * FROM raw.market_prices
WHERE date = CURRENT_DATE()
ORDER BY symbol;
```

### Get 14-Day Forecast
```sql
SELECT * FROM ANALYTICS.market_forecast_results
ORDER BY symbol, forecast_date;
```

### Compare Crypto vs Stocks
```sql
SELECT 
    asset_type,
    AVG(close) as avg_price,
    STDDEV(close) as volatility
FROM raw.market_prices
WHERE date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY asset_type;
```

## 🛠️ Troubleshooting

### Common Issues

**Issue**: DAG not appearing in Airflow
- Check DAG file syntax for errors
- Verify Python imports are correct
- Check Airflow logs in `logs/` directory

**Issue**: Snowflake connection fails
- Verify connection credentials in Airflow UI
- Check network connectivity to Snowflake
- Ensure warehouse is running

**Issue**: dbt run fails
- Verify `DBT_PROJECT_DIR` path is correct
- Check dbt profile configuration
- Ensure Snowflake schema exists

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is open source and available for educational purposes.

## 👥 Authors

- [Vaheedur Rehman](https://github.com/VaheedurRehman)
- Calvin Ha
- Saketh Nambiar

## 🙏 Acknowledgments

- Yahoo Finance for providing free market data
- Snowflake for ML capabilities
- Apache Airflow community
- dbt community

## 📧 Contact

For questions or feedback, please open an issue on GitHub.

---

**Note**: This project is for educational purposes. Always verify data accuracy and perform your own research before making investment decisions.
