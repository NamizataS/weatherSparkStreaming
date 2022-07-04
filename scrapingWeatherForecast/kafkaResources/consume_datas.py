from KafkaResources import KafkaInteraction

if __name__ == "__main__":
    kafkaInteractions = KafkaInteraction()
    kafkaInteractions.get_message("avg_weather")
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
            "window": {
                "type": "text"
            },
            "avg(temperatureFormatted)": {
                "type": "double"
            }
        }
    }
