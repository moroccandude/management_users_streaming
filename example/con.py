from confluent_kafka import Consumer
import json 
# Kafka consumer configuration
consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "my_group",
    "auto.offset.reset": "earliest",  # Start reading from the beginning
}

consumer = Consumer(consumer_conf)
consumer.subscribe(["wwww"])  # Subscribe to the topic

print("Waiting for messages...")

try:
 while True:
        msg = consumer.poll(1.0)  # Wait for a new message
      
        if msg is None:
             continue
        print(json.laods(msg))
        if msg.error():
             print(f"Consumer error: {msg.error()}")
             continue
         
        print(f"Received message:offset =>{msg.offset()} ")
            #    { msg.topic()} key =>{msg.key().decode('utf-8')} value =>{msg.value()}
           
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
