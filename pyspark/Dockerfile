FROM apache/spark-py:latest

# Install the Kafka connector
RUN /opt/spark/bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
COPY spark_consumer.py /opt/spark/apps/
WORKDIR /opt/spark/apps/
CMD ["spark-submit", "spark_consumer.py"]