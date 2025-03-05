from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "0.0.0.0:9092",
    "group.id": "my_group",
     'enable.auto.commit': True,
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["users"])

while True:
    msg = consumer.poll(1.0)  # Wait up to 1 second for a message
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    print(f"Received message: {msg.value().decode('utf-8')}")

consumer.close()
