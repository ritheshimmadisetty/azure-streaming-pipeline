# Azure Real-Time Streaming Pipeline

A real-time data engineering pipeline built on Azure — events flow from a Kafka producer through Azure Event Hubs into Databricks Spark Structured Streaming, processed through a Medallion architecture, and loaded into a Snowflake data warehouse modeled with dbt.

## What this project does

Simulates a live e-commerce platform generating order and clickstream events in real time. Every 2 seconds, new events are produced and streamed through the pipeline automatically.

## Tech Stack

- **Streaming:** Apache Kafka, Azure Event Hubs
- **Processing:** Azure Databricks, Apache Spark Structured Streaming, Delta Lake
- **Storage:** Azure Data Lake Storage Gen2 (Bronze/Silver/Gold)
- **Warehouse:** Snowflake
- **Transformation:** dbt
- **Orchestration:** Apache Airflow, Docker
- **CI/CD:** GitHub Actions
- **Version Control:** Git, GitHub

## Pipeline Flow
```
Producer (orders + clicks every 2s)
         ↓
Apache Kafka → Azure Event Hubs
         ↓
Databricks Spark Structured Streaming
         ↓
Bronze (raw events) → Silver (cleaned) → Gold (aggregated)
         ↓
Snowflake — dbt Star Schema
         ↓
fact_orders, fact_clicks, dim_customer, dim_product, dim_date
```

## How to run
```bash
# Start event producer
python producer.py

# Run dbt models
cd streaming_dw
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Author

Rithesh Immadisetty — Data Engineer, Bengaluru
[LinkedIn](https://linkedin.com/in/ritheshimmadisetty)