from loguru import logger
from pymongo import MongoClient


class CustomMongoClient:

    def __init__(self, uri, db):
        logger.info("Connection to db")
        self._cluster = MongoClient(uri)
        self._db = self._cluster[db]

    def update(self, collection, document: dict, id):
        collection = self._db[collection]
        filter = {"id": id}
        query = {'$set': document}
        collection.update_one(filter, query, upsert=True)

    def push_data(self, collection, document: dict, id):
        collection = self._db[collection]
        filter = {"id": id}
        query = {'$push': document}
        collection.update_one(filter, query, upsert=True)

    def take_data(self, collection, id):
        collection = self._db[collection]
        filter = {"id": id}
        data = collection.find_one(filter)
        return data





