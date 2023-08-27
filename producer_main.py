import sys,os
sys.path.append(os.getcwd())

from src.kafka_producer.json_producer import produce_data_using_file

import os

if __name__=="__main__":
    topics=os.listdir("data")
    for topic in topics:
        print(topic)
        sample_filepath=os.path.join("data",topic,os.listdir(f"data/{topic}")[0])
        produce_data_using_file(topic=topic,filepath=sample_filepath)