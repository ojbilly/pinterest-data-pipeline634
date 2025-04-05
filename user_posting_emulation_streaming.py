import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import yaml
import sqlalchemy
from sqlalchemy import text
from datetime import datetime, date

random.seed(100)


class AWSDBConnector:
    def __init__(self):
        print("🔧 Loading DB credentials...")
        with open("db_creds.yaml", "r") as file:
            creds = yaml.safe_load(file)["database"]

        print("✅ Credentials loaded")
        self.HOST = creds["host"]
        self.USER = creds["user"]
        self.PASSWORD = creds["password"]
        self.DATABASE = creds["database"]
        self.PORT = creds["port"]

    def create_db_connector(self):
        print("🔌 Creating DB connection engine...")
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        print("✅ Engine created")
        return engine


new_connector = AWSDBConnector()


# Custom JSON serializer for datetime objects
def json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def send_to_kinesis(payload: dict, partition_key: str, invoke_url: str):
    headers = {"Content-Type": "application/json"}
    url = invoke_url

    data = {
    "Data": json.dumps(payload, default=json_serializer),
    "PartitionKey": partition_key
    }


    print(f"📤 Sending to Kinesis | URL: {url}")
    print(f"🧾 Payload: {json.dumps(data, indent=2)}")  # Debug

    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print(f"🔁 Status: {response.status_code}")
        print(f"🔙 Response: {response.text}")
    except Exception as e:
        print(f"❌ Exception: {e}")



if __name__ == "__main__":
    print("🚀 Starting data emulation to Kinesis")
    engine = new_connector.create_db_connector()
    invoke_url = "https://y74e4z45o1.execute-api.eu-west-1.amazonaws.com/prod/send"


    for i in range(5):  # test with smaller number first
        print(f"\n📦 Iteration {i + 1}")
        random_row = random.randint(0, 11000)

        try:
            print("🔗 Connecting to DB...")
            with engine.connect() as connection:
                print("🔍 Fetching pin_result...")
                pin_result = [dict(row._mapping) for row in connection.execute(
                    text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"))][0]

                print("🔍 Fetching geo_result...")
                geo_result = [dict(row._mapping) for row in connection.execute(
                    text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1"))][0]

                print("🔍 Fetching user_result...")
                user_result = [dict(row._mapping) for row in connection.execute(
                    text(f"SELECT * FROM user_data LIMIT {random_row}, 1"))][0]

            print("📨 Sending records...")
            send_to_kinesis(pin_result, "pin", invoke_url)
            send_to_kinesis(geo_result, "geo", invoke_url)
            send_to_kinesis(user_result, "user", invoke_url)

        except Exception as e:
            print(f"❌ Error during iteration {i + 1}: {e}")

        sleep(0.3)  # slight delay to avoid overloading the API

    print("🎉 Completed sending all data.")