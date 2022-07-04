from json import loads
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic


class KafkaInteraction:
    def __init__(self):
        self.bootstrap_servers = "localhost:29092"
        self.client_id = "test"
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers, batch_size=400000,
                                      api_version=(2, 8, 1))

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

    def generate_data(self, docs):
        for docu in docs:
            yield {
                "_index": "avg_weather",
                "_source": {k: v if v else None for k, v in docu.items()},
            }

    def get_message(self, topic, es):
        consumer = KafkaConsumer(topic, bootstrap_servers=[self.bootstrap_servers], enable_auto_commit=False,
                                 auto_offset_reset='latest', value_deserializer=lambda x: loads(x.decode('utf-8')))
        print("Start to check topic")
        consumer.poll()
        for message in consumer:
            message = message.value
            print(message)
            try:
                formattedMessage = {"city": message['city'], "country": message['country'],
                                    "lat": message["lat"], "lng": message["lng"],
                                    'start': message['window']["start"], 'end': message['window']["end"],
                                    "avg(temperatureFormatted)": message['avg(temperatureFormatted)']}
                bulk(es, self.generate_data([formattedMessage]))
            except KeyError:
                continue
            print("Sending message to elastic search")
        consumer.commit()


if __name__ == "__main__":
    mapping = {
        "properties": {
            "city": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "country": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "lat": {
              "type": "text"
            },
            "lng": {
              "type": "text"
            },
            "start": {
                "type": "date"
            },
            "end": {
              "type": "date"
            },
            "avg(temperatureFormatted)": {
                "type": "double"
            }
        }
    }
    es_client = Elasticsearch(hosts="http://localhost:9200/")
    es_client.options(ignore_status=[400, 401, 404]).indices.delete(index="avg_weather")
    settings = {"max_result_window": 10000000}
    es_client.indices.create(index="avg_weather", mappings=mapping, settings=settings)
    kafkaInteractions = KafkaInteraction()
    kafkaInteractions.get_message("avg_weather", es_client)
