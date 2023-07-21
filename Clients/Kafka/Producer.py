from kafka import KafkaProducer
from loguru import logger

from Clients.Kafka import KafkaCoreClient


class KafkaProducerClient(KafkaCoreClient):
    def __init__(self, servers):
        super().__init__(servers)
        self._producer = None
        self._connect()

    def _connect(self):
        _producer = None
        try:
            _producer = KafkaProducer(bootstrap_servers=self._servers, api_version=(0, 10))
        except Exception as ex:
            logger.error(f'Exception while connecting Kafka, err = {ex}')
        finally:
            self._producer = _producer

    def publish_message(self, topic_name, value):
        try:
            value_bytes = bytes(value, encoding='utf-8')
            self._producer.send(topic_name, value=value_bytes)
        except Exception as e:
            logger.error(f"Error at publishing message on topic {topic_name}, err = {e}")

    def __del__(self):
        if self._producer is not None:
            self._producer.close()
