import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse='DE_WH',
    database='DE_PROJECT',
    schema='STREAMING'
)

cursor = conn.cursor()

# Create raw tables
cursor.execute("""
    CREATE OR REPLACE TABLE RAW_ORDERS (
        event_type      VARCHAR,
        order_id        VARCHAR,
        customer_id     INTEGER,
        customer_name   VARCHAR,
        city            VARCHAR,
        product_id      INTEGER,
        product_name    VARCHAR,
        category        VARCHAR,
        unit_price      FLOAT,
        quantity        INTEGER,
        total_amount    FLOAT,
        status          VARCHAR,
        platform        VARCHAR,
        event_timestamp TIMESTAMP,
        event_date      DATE,
        event_hour      INTEGER,
        event_year      INTEGER,
        event_month     INTEGER,
        processed_at    TIMESTAMP,
        ingested_at     TIMESTAMP
    )
""")

cursor.execute("""
    CREATE OR REPLACE TABLE RAW_CLICKS (
        event_type      VARCHAR,
        session_id      VARCHAR,
        customer_id     INTEGER,
        page            VARCHAR,
        product_id      INTEGER,
        action          VARCHAR,
        city            VARCHAR,
        device          VARCHAR,
        event_timestamp TIMESTAMP,
        event_date      DATE,
        event_hour      INTEGER,
        processed_at    TIMESTAMP,
        ingested_at     TIMESTAMP
    )
""")

print("✅ Snowflake tables created")

# Load orders
orders_df = pd.read_csv('streaming_orders.csv')
orders_df.columns = [c.upper() for c in orders_df.columns]
write_pandas(conn, orders_df, 'RAW_ORDERS', quote_identifiers=False)
print(f"✅ Loaded {len(orders_df)} orders into Snowflake")

# Load clicks
clicks_df = pd.read_csv('streaming_clicks.csv')
clicks_df.columns = [c.upper() for c in clicks_df.columns]
write_pandas(conn, clicks_df, 'RAW_CLICKS', quote_identifiers=False)
print(f"✅ Loaded {len(clicks_df)} clicks into Snowflake")

cursor.close()
conn.close()
print("\n✅ All streaming data loaded into Snowflake STREAMING schema!")