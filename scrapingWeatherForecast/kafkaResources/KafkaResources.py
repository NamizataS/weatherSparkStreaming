from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession


class KafkaInteraction:
    def __init__(self):
        self.bootstrap_servers = "localhost:29092"
        self.client_id = "test"
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers, batch_size=400000)

    def create_topic(self, name):
        admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers, client_id="test")
        topic_list = [NewTopic(name=name, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    def send_message(self, message, topic):
        print("Starting to send message to topic")
        for mes in message:
            self.producer.send(topic, mes)
        self.producer.flush(timeout=10000)

    def close_producer_connection(self):
        self.producer.close(timeout=5)
