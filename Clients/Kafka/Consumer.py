import json
from json.decoder import JSONDecodeError
from typing import List
from errors import JSONParseError
from Clients.Kafka import KafkaCoreClient
from kafka import KafkaConsumer
from loguru import logger


class KafkaConsumerClient(KafkaCoreClient):
    def __init__(self, servers):
        super().__init__(servers)

    def read_messages(self, topic: str):
        logger.info("Connecting to the kafka server")
        consumer = KafkaConsumer(topic, bootstrap_servers=self._servers,
                                 auto_offset_reset='earliest',
                                 api_version=(0, 10),)

        for msg in consumer:
            try:
                value = json.loads(msg.value.decode('utf-8'))
                logger.debug(f"take this massage {value} from topic {topic}")
                yield value
            except JSONDecodeError as e:
                logger.error(f"Cannot parse message as json {msg}, err= {e}")

        if consumer is not None:
            consumer.close()
            logger.info("Closing the connection")

    def one_time_read_messages(self, topic: str) -> List:
        logger.info("Connecting to the kafka server")
        consumer = KafkaConsumer(topic, bootstrap_servers=self._servers,
                                 auto_offset_reset='earliest',
                                 api_version=(0, 10),
                                 consumer_timeout_ms=1000)

        messages = []
        for msg in consumer:
            try:
                value = msg.value.decode('utf-8')
                logger.debug(f"take this massage {value} from topic {topic}")
                messages.append(value)
            except json.decoder.JSONDecodeError as e:
                logger.error(f"Cannot parse message {msg}, err= {e}")

        if consumer is not None:
            consumer.close()
            logger.info("Closing the connection")

        return messages
