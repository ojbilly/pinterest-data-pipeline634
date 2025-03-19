# Pinterest Data Pipeline

## Table of Contents
- [Project Description](#project-description)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
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
- Document all progress in a structured README file.

### **What I Learned:**
- Setting up and connecting to an **AWS EC2 instance** using SSH.
- Managing **Kafka topics** for real-time data streaming.
- Storing sensitive credentials securely in **YAML files**.
- Using **AWS Systems Manager** to execute commands remotely.

---

## Installation Instructions
### **Step 1: Create a Key Pair**
1. Navigate to **AWS Systems Manager > Parameter Store**.
2. Find the key pair associated with your EC2 instance using **KeyPairId**.
3. Copy the key value and save it as `KeyPairName.pem` in **VS Code**.

### **Step 2: Connect to the EC2 Instance**
Run the following commands in your terminal:
```bash
chmod 400 KeyPairName.pem
ssh -i KeyPairName.pem ec2-user@<your-ec2-public-ip>
```

### **Step 3: Verify Kafka Installation**
Once logged in, check if Kafka is installed:
```bash
cd ~/kafka
ls -l
```
You should see Kafka-related files and folders.

---

## Usage Instructions
### **Step 1: Create Kafka Topics**
Kafka topics were created to handle different types of Pinterest data:
```bash
bin/kafka-topics.sh --create --topic <your_UserId>.pin --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic <your_UserId>.geo --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic <your_UserId>.user --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
To verify:
```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### **Step 2: Modify Kafka Configuration for S3 Sink Connector**
1. Navigate to the Kafka configuration directory:
   ```bash
   cd /etc/kafka
   ```
2. Open the `s3-sink-connector.properties` file:
   ```bash
   sudo nano s3-sink-connector.properties
   ```
3. Update the following values:
   ```ini
   s3.region=eu-west-1
   topics=<your_UserId>.pin,<your_UserId>.geo,<your_UserId>.user
   s3.bucket.name=your-bucket-name
   ```
4. Save and exit.
5. Restart Kafka Connect:
   ```bash
   sudo systemctl stop kafka-connect.service
   sudo systemctl start kafka-connect.service
   sudo systemctl status kafka-connect.service
   ```

### **Step 3: Retrieve Data from the Database**
- The `user_posting_emulation.py` script connects to an **AWS RDS database** that contains three tables:
  - `pinterest_data`: Contains posts.
  - `geolocation_data`: Stores post geolocation.
  - `user_data`: Contains user details.

- Credentials are stored in a separate **`db_creds.yaml`** file to prevent leaks.
- This file is **added to .gitignore** to avoid committing sensitive data.

To run the script and print one entry from each table:
```bash
python3 user_posting_emulation.py
```

---

## File Structure
```
|-- pinterest-data-pipeline/
|   |-- kafka/                      # Kafka installation directory
|   |-- user_posting_emulation.py   # Script to fetch and send data to Kafka
|   |-- db_creds.yaml               # Secure database credentials file (gitignored)
|   |-- README.md                   # Documentation (this file)
```

---

## Security Considerations
- **Never commit credentials**: `db_creds.yaml` is added to **.gitignore**.
- **EC2 Security Groups** allow SSH (port 22) **only from trusted IPs**.
- **Kafka topics have restricted access** to prevent unauthorized use.

---

## License
This project is licensed under the **Osaze Omoruyi**.

---

This README will be continuously updated as more progress is made! 🚀
```

