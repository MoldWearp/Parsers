import json
import os
from bson import json_util
from dotenv import load_dotenv
from Clients.Kafka import KafkaProducerClient
from loguru import logger


load_dotenv()

kafka_url = os.getenv("KAFKA_URL")

broker = KafkaProducerClient(kafka_url)


class VKPipeline:
    async def process_item(self, item, spider):
        logger.debug(f"item = {item}")
        item["metadata"]["cur_node"] += 1

        json_kafka_message = {"id": item["id"], "metadata": item["metadata"]}
        kafka_message = json.dumps(json_kafka_message, default=json_util.default)

        broker.publish_message("queue", kafka_message)
