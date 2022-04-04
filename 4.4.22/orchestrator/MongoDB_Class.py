import os
from pymongo import MongoClient

class MongoDB_Class:
    MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
    MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))

    def __init__(self):
        mongo_host = MongoDB_Class.MONGO_HOST+":"+str(MongoDB_Class.MONGO_PORT)
        self.mongo_client = MongoClient("mongodb://"+mongo_host+"/")
        return
    
    def insertMongoRecord(self, client, collection, record):
        mongo_db = self.mongo_client[client]
        analysis_collection = mongo_db[collection]
        analysis_collection.insert_one(record)
        return