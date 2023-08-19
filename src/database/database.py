import pymongo
import os
import sys
sys.path.append(os.getcwd())
from src.kafka_logger.logger import logging



class MongodbOps:
    def __init__(self):
        self.client=pymongo.MongoClient("mongodb+srv://rohanpatankar926:1234@sensorkafka.w1pvkpx.mongodb.net/")
        self.database_name="sensorkafka"
        logging.info(f"database name tracked {self.database_name}")
    
    def insert_many(self,collection_name:str,records:list):
        #inserting the data which has multiple key value pair or batch json
        logging.info("inserting the batch records")
        self.client[self.database_name][collection_name].insert_many(records)

    def insert_single(self,collection_name:str,records:dict):
        #inserting only single key value pair or json
        logging.info("inserting the single records")
        self.client[self.database_name][collection_name].insert_one(records)


data=[{"name":"rohan"},{"name":"tushar"}]

MongodbOps().insert_many("single_test",data)