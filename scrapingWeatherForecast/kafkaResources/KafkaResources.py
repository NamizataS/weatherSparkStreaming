from json import loads
from elasticsearch import Elasticsearch
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

    def get_message(self, topic, es, index):
        consumer = KafkaConsumer(topic, bootstrap_servers=[self.bootstrap_servers], enable_auto_commit=False,
                                 auto_offset_reset='latest', value_deserializer=lambda x: loads(x.decode('utf-8')))
        print("Start to check topic")
        consumer.poll()
        i = 0
        for message in consumer:
            message = message.value
            print(message)
            try:
                match topic:
                    case "avg_weather":
                        formattedMessage = {"city": message['city'], "country": message['country'],
                                            "lat": message["lat"], "lng": message["lng"],
                                            'start': message['window']["start"], 'end': message['window']["end"],
                                            "avg(temperatureFormatted)": message['avg(temperatureFormatted)']}
                    case "clean_datas":
                        formattedMessage = {"city": message['city'], "country": message['country'],
                                            "sky_status": message['sky_status'], "date_formatted": message['date_formatted'],
                                            "temperatureFormatted": message['temperatureFormatted'], "lat": message["lat"],
                                            "lng": message["lng"]}
                es.index(index=index, id=i, body=formattedMessage)
                i += 1
            except KeyError as e:
                print(f"Error is {e}")
                continue
            print("Sending message to elastic search")
        consumer.commit()
