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
        # Load credentials from YAML file
        with open("db_creds.yaml", "r") as file:
            creds = yaml.safe_load(file)["database"]

        self.HOST = creds["host"]
        self.USER = creds["user"]
        self.PASSWORD = creds["password"]
        self.DATABASE = creds["database"]
        self.PORT = creds["port"]

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine


def json_serializer(obj):
    """Custom JSON serializer for datetime objects."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def send_to_kafka(topic: str, payload: dict, invoke_url: str):
    headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
    url = f"{invoke_url}/{topic}"
    data = {"records": [{"value": payload}]}

    try:
        response = requests.post(url, headers=headers, data=json.dumps(data, default=json_serializer))
        if response.ok:
            print(f"Sent to {topic}")
        else:
            print(f"Failed to send to {topic}: {response.text}")
    except Exception as e:
        print(f"Exception sending to {topic}: {e}")


if __name__ == "__main__":
    new_connector = AWSDBConnector()
    engine = new_connector.create_db_connector()
    invoke_url = "https://mqkxd7hag2.execute-api.eu-west-1.amazonaws.com/dev/topics"

    for _ in range(500):
        random_row = random.randint(0, 11000)

        with engine.connect() as connection:
            pin_result = [dict(row._mapping) for row in connection.execute(
                text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"))][0]

            geo_result = [dict(row._mapping) for row in connection.execute(
                text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1"))][0]

            user_result = [dict(row._mapping) for row in connection.execute(
                text(f"SELECT * FROM user_data LIMIT {random_row}, 1"))][0]

        print(pin_result)
        send_to_kafka("testbuck1.pin", pin_result, invoke_url)
        send_to_kafka("testbuck1.geo", geo_result, invoke_url)
        send_to_kafka("testbuck1.user", user_result, invoke_url)

        sleep(0.3)

    print("Completed sending all data.")
