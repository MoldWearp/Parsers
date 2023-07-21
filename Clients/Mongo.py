from loguru import logger
from pymongo import MongoClient


class CustomMongoClient:
    def __init__(self, uri, db):
        logger.info("Connection to db")
        self._cluster = MongoClient(uri)
        self._db = self._cluster[db]

    def insert(self, collection, document):
        collection = self._db[collection]
        filter = {"id": document["id"]}
        collection.update_one(filter, document, upsert=True)




