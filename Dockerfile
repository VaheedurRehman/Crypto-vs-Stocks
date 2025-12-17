FROM apache/airflow:2.7.1-python3.9

# Install dbt-snowflake and yfinance (required by your teammates' code)
# We also install the snowflake connector to ensure python can talk to the DB
RUN pip install --no-cache-dir \
    dbt-snowflake \
    yfinance \
    pandas \
    snowflake-connector-python \
    snowflake-snowpark-python