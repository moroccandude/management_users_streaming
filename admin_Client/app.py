from confluent_kafka.admin import AdminClient, NewTopic

# Kafka configuration
admin_client = AdminClient({"bootstrap.servers": "kafka:9092"})

topic_name = "users"
new_topic = NewTopic(topic_name, num_partitions=3, replication_factor=1)

# Create the topic
fs = admin_client.create_topics([new_topic])

# Check if the topic was created successfully
for topic, f in fs.items():
    try:
        f.result()  # If successful, this won't raise an exception
        print(f"Topic '{topic}' created successfully!")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")
