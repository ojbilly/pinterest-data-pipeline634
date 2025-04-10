# Pinterest Data Pipeline

## Table of Contents
- [Project Description](#project-description)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [AWS Resources Setup](#aws-resources-setup)
- [Databricks Integration](#databricks-integration)
- [Airflow Automation](#airflow-automation)
- [Streaming via Kinesis & Firehose](#streaming-via-kinesis--firehose)
- [File Structure](#file-structure)
- [Security Considerations](#security-considerations)
- [License](#license)

---

## Project Description
This project sets up a Pinterest-style Data Pipeline using AWS (EC2, RDS, S3, Kinesis), Kafka, Apache Airflow, and Databricks. The system streams, stores, processes, and analyzes user posting data.

### Objectives
- Launch an EC2 instance and set up Kafka
- Create Kafka topics and use REST Proxy for posting
- Use an RDS MySQL database with Pinterest-style data
- Configure S3 buckets and Kafka S3 Sink Connector
- Use Databricks for transformation and analysis
- Orchestrate pipelines with Airflow and visualize data

---

## Installation Instructions

### 1. Key Pair Setup
```bash
chmod 400 KeyPairName.pem
ssh -i KeyPairName.pem ubuntu@<ec2-public-ip>
```

### 2. Kafka Verification
```bash
cd ~/kafka
ls -l
```

---

## Usage Instructions

### 1. Create Kafka Topics
```bash
bin/kafka-topics.sh --create --topic <UserId>.pin --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic <UserId>.geo --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic <UserId>.user --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 2. Start Services
```bash
# Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Kafka
bin/kafka-server-start.sh config/server.properties

# Kafka Connect
bin/connect-distributed.sh config/connect-distributed.properties
```

### 3. Run Data Emulation
```bash
python3 user_posting_emulation.py
```

### 4. Monitor
```bash
journalctl -u kafka-rest.service -f
```

---

## AWS Resources Setup

### RDS (MySQL)
- Created via AWS Console
- Populated using `pinterest_data_db.sql`

### S3 Bucket
- Bucket name: `kinesis-s3-databrick`
- Configured IAM and connector to automatically structure streamed data paths

---

## Databricks Integration

### 1. Notebook Development
- Free Community Databricks account
- Created notebooks for pulling, cleaning, and saving data

### 2. Load S3 Data
```python
spark.read.json("s3a://kinesis-s3-databrick/<topic-path>")
```

### 3. Cleaning & Analysis
- Converted follower counts (e.g., 79k → 79000)
- Cleaned null values, deduplicated by `unique_id`
- Used Spark DataFrame transformations

### 4. Saved to Delta Tables
```python
df.write.format("delta").mode("overwrite").saveAsTable("<UserId>_pin_table")
```

---

## Airflow Automation

### 1. Airflow Local Setup
```bash
python -m venv airflow_venv
source airflow_venv/bin/activate
pip install apache-airflow
export AIRFLOW_HOME=~/airflow
airflow db init
```

### 2. DAG and Execution
- Created a DAG to run exported Databricks notebooks using `PythonOperator`

---

## Streaming via Kinesis & Firehose

### 1. Stream to Kinesis
- Created `user_posting_emulation_streaming.py`
- Posted data using API Gateway to `my-pin-stream`

### 2. Connect Firehose to S3
- Configured delivery stream: `KDS-S3-gjcBX`
- Data lands in `s3a://kinesis-s3-databrick/`

### 3. Databricks
- Pulled streaming data from S3
- Cleaned and transformed in notebooks
- Saved to Delta tables

---

## File Structure
```
|-- pinterest-data-pipeline/
|   |-- kafka/
|   |-- user_posting_emulation.py
|   |-- user_posting_emulation_streaming.py
|   |-- db_creds.yaml
|   |-- dags/
|   |   |-- run_notebook_dag.py
|   |   |-- scripts/
|   |-- notebooks/
|   |   |-- 1_pull_stream_data.py
|   |   |-- 2_clean_stream_data.py
|   |   |-- 3_save_delta_tables.py
|   |-- README.md
```

---

## Security Considerations
- Secrets like RDS and S3 credentials stored in `.yaml` or environment variables
- IAM roles scoped to specific services
- Access restricted via API Gateway and VPC security groups

---

## License
Project maintained by **Osaze Omoruyi**.

---

🚀 Project delivered with AWS, Apache, and Databricks technologies.
