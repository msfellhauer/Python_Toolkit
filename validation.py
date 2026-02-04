snowflake_validation_toolkit/
│
├── config.py
├── validation.py
├── run_validations.py
├── logs/
│   └── validation.log

#Configuration Check for Snowflake 
import os
import snowflake.connector

def get_connection():
    #"""
    #Establish a connection to Snowflake using environment variables.
    #"""
    return snowflake.connector.connect(
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA")
    )
#Data validation check, utilizing pandas. There are multiple validation checks within this program 
#where these can be run separately or as a group. 

import pandas as pd

# -------------------
# Duplicate Key Check
# -------------------
def check_duplicates(conn, schema, table, key_column):
    query = f"""
        SELECT {key_column}, COUNT(*) AS cnt
        FROM {schema}.{table}
        GROUP BY {key_column}
        HAVING COUNT(*) > 1
    """
    df = pd.read_sql(query, conn)
    return df

# -------------------
# Null Value Check
# -------------------
def check_nulls(conn, schema, table):
    cursor = conn.cursor()
    query = f"SELECT * FROM {schema}.{table} LIMIT 0"
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    
    null_counts = {}
    for col in columns:
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table} WHERE {col} IS NULL")
        null_counts[col] = cursor.fetchone()[0]
    return null_counts

# -------------------
# Referential Integrity
# -------------------
def check_referential_integrity(conn, parent_table, child_table, parent_key, child_key, schema):
    query = f"""
        SELECT c.*
        FROM {schema}.{child_table} c
        LEFT JOIN {schema}.{parent_table} p
        ON c.{child_key} = p.{parent_key}
        WHERE p.{parent_key} IS NULL
    """
    df = pd.read_sql(query, conn)
    return df

# -------------------
# Row Count Comparison
# -------------------
def compare_row_counts(conn, table1, table2, schema):
    query = f"""
        SELECT
            (SELECT COUNT(*) FROM {schema}.{table1}) AS table1_count,
            (SELECT COUNT(*) FROM {schema}.{table2}) AS table2_count
    """
    cursor = conn.cursor()
    cursor.execute(query)
    row1_count, row2_count = cursor.fetchone()
    return row1_count, row2_count

# -------------------
# Outlier / Range Check
# -------------------
def check_outliers(conn, schema, table, column, min_val, max_val):
    query = f"""
        SELECT *
        FROM {schema}.{table}
        WHERE {column} < {min_val} OR {column} > {max_val}
    """
    df = pd.read_sql(query, conn)
    return df

# -------------------
# Task Failure Check
# -------------------
def check_failed_tasks(conn, schema, days=7):
    query = f"""
        SELECT name, completed_time, state
        FROM TABLE(
            INFORMATION_SCHEMA.TASK_HISTORY(
                SCHEDULED_TIME_RANGE_START => DATEADD('DAY', -{days}, CURRENT_TIMESTAMP())
            )
        )
        WHERE schema_name = '{schema}' AND state = 'FAILED'
        ORDER BY completed_time DESC
    """
    df = pd.read_sql(query, conn)
    return df



# This will run the validations. Once that validation is ran, the determination on where to put that 
# can be included within the code. 

from config import get_connection
from validation import (
    check_duplicates,
    check_nulls,
    check_referential_integrity,
    compare_row_counts,
    check_outliers,
    check_failed_tasks
)

def main():
    conn = get_connection()
    schema = "MY_SCHEMA"

    print("=== Duplicate Check ===")
    print(check_duplicates(conn, schema, "ORDERS", "order_id"))

    print("\n=== Null Check ===")
    print(check_nulls(conn, schema, "ORDERS"))

    print("\n=== Referential Integrity ===")
    print(check_referential_integrity(conn, "CUSTOMERS", "ORDERS", "customer_id", "customer_id", schema))

    print("\n=== Row Count Comparison ===")
    print(compare_row_counts(conn, "ORDERS_STG", "ORDERS", schema))

    print("\n=== Outlier Check ===")
    print(check_outliers(conn, schema, "ORDERS", "amount", 0, 10000))

    print("\n=== Failed Tasks ===")
    print(check_failed_tasks(conn, schema, days=7))

    conn.close()

if __name__ == "__main__":
    main()

