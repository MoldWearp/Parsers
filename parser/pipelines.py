import json
import os

from bson import json_util
from dotenv import load_dotenv
from Clients.Kafka import KafkaProducerClient
from settings import db

load_dotenv()

kafka_url = os.getenv("KAFKA_URL")

broker = KafkaProducerClient(kafka_url)


class VKPipeline:
    async def process_item(self, item):
        graph = item["metadata"]["graph"]
        item["metadata"]["cur_node"] += 1
        cur_node = graph[item["metadata"]["cur_node"]]

        json_kafka_message = {"id": item["id"], "metadata": item["metadata"]}
        kafka_message = json.dumps(json_kafka_message, default=json_util.default)

        if item["metadata"]["cur_node"] > len(graph):
            cur_node = "Finish"

        match cur_node:
            case "UsersByGroup":
                broker.publish_message("UsersByGroup", kafka_message)
            case "VKFriends":
                broker.publish_message("VkFriends", kafka_message)
            case "VKGroupsByUser":
                broker.publish_message("GroupsByUser", kafka_message)
            case "VKUser":
                broker.publish_message("VKUser", kafka_message)
            case "Finish":
                broker.publish_message("Finish", kafka_message)
