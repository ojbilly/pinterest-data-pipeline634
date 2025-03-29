# Pinterest Data Pipeline

## Table of Contents
- [Project Description](#project-description)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [AWS Resources Setup](#aws-resources-setup)
- [File Structure](#file-structure)
- [Security Considerations](#security-considerations)
- [License](#license)

---

## Project Description
This project involves setting up a **Pinterest Data Pipeline** on an AWS EC2 instance using **Kafka** to process user posting data. Pinterest processes billions of data points daily, and this project aims to simulate a similar system using **AWS Cloud**.

### **Objectives:**
- Set up and connect to an **EC2 instance** with Kafka pre-installed.
- Create **Kafka topics** to process Pinterest post data.
- Securely store database credentials in a separate file.
- Use **RDS database** containing Pinterest-like data.
- Create and configure **S3 bucket** to store streamed data.
- Use **Kafka REST Proxy** and **S3 Sink Connector** for data streaming.
- Document all progress in a structured README file.

### **What I Learned:**
- Setting up and connecting to an **AWS EC2 instance** using SSH.
- Managing **Kafka topics** for real-time data streaming.
- Creating **MySQL RDS instances** and importing data.
- Creating and configuring an **S3 bucket** with proper IAM roles.
- Installing and using **Kafka Connect** and the **S3 Sink Connector**.
- Sending data through a **Kafka REST Proxy** endpoint to topics.
- Storing sensitive credentials securely in **YAML files**.
- Using **AWS Systems Manager** to execute commands remotely.

---

## Installation Instructions
### **Step 1: Create a Key Pair**
1. Navigate to **AWS EC2 > Key Pairs** and create a new key pair.
2. Save it as `KeyPairName.pem` locally and set permissions:

```bash
chmod 400 KeyPairName.pem
```

### **Step 2: Connect to the EC2 Instance**
```bash
ssh -i KeyPairName.pem ubuntu@<your-ec2-public-ip>
```

### **Step 3: Verify Kafka Installation**
Once logged in:
```bash
cd ~/kafka
ls -l
```
You should see Kafka-related files and folders like `zookeeper-server-start.sh`, `kafka-server-start.sh`, etc.

---

## Usage Instructions

### **Step 1: Create Kafka Topics**
```bash
bin/kafka-topics.sh --create --topic <your_UserId>.pin --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic <your_UserId>.geo --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic <your_UserId>.user --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
To verify:
```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### **Step 2: Run Zookeeper and Kafka in Terminals**
Use **multiple terminal tabs** on EC2:
```bash
# Tab 1 - Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Tab 2 - Kafka
bin/kafka-server-start.sh config/server.properties

# Tab 3 - Kafka Connect
bin/connect-distributed.sh config/connect-distributed.properties
```

### **Step 3: Modify user_posting_emulation.py**
This script reads 500 random rows from your database and pushes them to the appropriate Kafka topics via **Kafka REST Proxy**.
Run the script:
```bash
python3 user_posting_emulation.py
```

### **Step 4: Monitor Data Sending**
On EC2, run:
```bash
journalctl -u kafka-rest.service -f
```

---

## AWS Resources Setup

### **RDS (MySQL)**
- Created **MySQL RDS (Free Tier)** via AWS Console.
- Used **MySQL Workbench** locally to:
  - Connect using RDS endpoint.
  - Create schema `pinterest_data`.
  - Import `pinterest_data_db.sql` to populate tables.

### **S3 Bucket**
- Created S3 bucket `testbuck1` via AWS Console.
- Configured IAM role with `AmazonS3FullAccess` attached to EC2 instance.
- Bucket path auto-structured by connector as:
```
topics/<your_UserId>.pin/partition=0/
topics/<your_UserId>.geo/partition=0/
topics/<your_UserId>.user/partition=0/
```
---

## File Structure
```
|-- pinterest-data-pipeline/
|   |-- kafka/                        # Kafka installation directory
|   |-- user_posting_emulation.py    # Script to fetch and send data to Kafka
|   |-- db_creds.yaml                # Secure database credentials file (gitignored)
|   |-- README.md                    # Documentation (this file)
```

---

## Security Considerations
- **Never commit credentials**: `db_creds.yaml` is added to **.gitignore**.
- **IAM role attached to EC2** securely grants access to S3.
- **Kafka topics access** limited to local EC2 or secured proxies.
- **MySQL RDS access** limited via **security group rules**.

---

## License
This project is licensed under the **Osaze Omoruyi**.

---

This README will be continuously updated as more progress is made! 🚀

