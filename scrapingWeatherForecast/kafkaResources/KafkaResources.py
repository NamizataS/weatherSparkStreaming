from json import loads

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic


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
        print("Finished sending messages to topic")

    def close_producer_connection(self):
        self.producer.close(timeout=5)

    def get_message(self, topic):
        consumer = KafkaConsumer(topic, bootstrap_servers=[self.bootstrap_servers], enable_auto_commit=False,
                                 auto_offset_reset='latest', value_deserializer=lambda x: loads(x.decode('utf-8')))
        print("Start to check topic")
        consumer.poll()
        for message in consumer:
            message = message.value
            print(message)
        consumer.commit()


if __name__ == "__main__":
    kafkaInteractions = KafkaInteraction()
    kafkaInteractions.get_message("avg_weather")
