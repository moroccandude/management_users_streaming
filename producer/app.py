from confluent_kafka import Producer
import json
import requests
import time

# Kafka configuration
KAFKA_BROKER = "localhost:9092"  # Ensure this matches your Kafka container settings
TOPIC_NAME = "users"

# Configure the Kafka Producer
producer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "acks": "all",  # Ensure all replicas acknowledge the message
    "retries": 3  # Retries in case of transient failures
}
producer = Producer(producer_config)

def fetch_data():
    """
    Fetches random user data from an external API.
    Returns JSON data if successful, None otherwise.
    """
    try:
        response = requests.get("https://randomuser.me/api/?results=10")
        response.raise_for_status()  # Ensure request was successful
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None  # Return None to handle failure cases

def delivery_report(err, msg):
    """
    Callback function to confirm message delivery.
    """
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_messages():
    """
    Fetches user data and sends it to the Kafka topic.
    """
    users_data = fetch_data()
    if users_data:
        for user in users_data.get("results", []):
            user_json = json.dumps(user)
            print(f"{user_json} \n_______________________________" )
            producer.produce(TOPIC_NAME, key=str(user["login"]["uuid"]), value=user_json, callback=delivery_report)
            time.sleep(0.6)  # Small delay to simulate a real-time stream
            producer.flush()  # Ensure all messages are sent before exiting

if __name__ == "__main__":
    produce_messages()
