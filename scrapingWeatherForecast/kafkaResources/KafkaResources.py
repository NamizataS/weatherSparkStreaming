from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


class KafkaInteraction:
    def __init__(self):
        self.bootstrap_servers = "localhost:29092"
        self.client_id = "test"

    def create_topic(self, name):
        admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers, client_id="test")
        topic_list = [NewTopic(name=name, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    def send_message(self, message, topic):
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
        print("Starting to send topic")
        producer.send(topic, message)
        producer.flush(timeout=10)
        producer.close(timeout=5)


