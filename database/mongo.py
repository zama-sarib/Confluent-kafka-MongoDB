import pymongo
import os
import certifi

ca = certifi.where()

class MongodbOperation:
    def __init__(self) -> None:
        self.client = pymongo.MongoClient(os.getenv('Mongo_DB_URL'))
        self.db = "kafka-consumer-db"

    def insert_many(self,collection_name,records):
        self.client[self.db][collection_name].insert_many(records)
    
    def insert_one(self,collection_name,record):
        self.client[self.db][collection_name].insert_one(record)