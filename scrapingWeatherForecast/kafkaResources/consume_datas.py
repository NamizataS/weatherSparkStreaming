from KafkaResources import KafkaInteraction

if __name__ == "__main__":
    kafkaInteractions = KafkaInteraction()
    kafkaInteractions.get_message("avg_weather")
