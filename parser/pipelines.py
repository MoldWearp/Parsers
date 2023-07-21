import json
import os

from bson import json_util
from dotenv import load_dotenv
from Clients.Mongo import CustomMongoClient
from Clients.Kafka import KafkaProducerClient

load_dotenv()

uri = os.getenv("MONGO_URI")
mongo_db = os.getenv("MONGO_DB")
kafka_url = os.getenv("KAFKA_URL")

broker = KafkaProducerClient(kafka_url)
db = CustomMongoClient(uri, mongo_db)


class VKPipeline:
    async def process_item(self, item):
        graph = item["metadata"]["graph"]
        item["metadata"]["cur_node"] += 1
        cur_node = graph[item["metadata"]["cur_node"]]
        if item["metadata"]["cur_node"] > len(graph):
            cur_node = "Finish"

        match cur_node:
            case "UsersByGroup":
                broker.publish_message("UsersByGroup", item["id"])
                db.insert("storage", item)
            case "VKFriends":
                broker.publish_message("VkFriends", item["id"])
                db.insert("storage", item)
            case "VKGroupsByUser":
                broker.publish_message("GroupsByUser", item["id"])
                db.insert("storage", item)
            case "VKUser":
                broker.publish_message("VKUser", item["id"])
                db.insert("storage", item)
            case "Finish":
                broker.publish_message("Finish", item["id"])
                db.insert("storage", item)
