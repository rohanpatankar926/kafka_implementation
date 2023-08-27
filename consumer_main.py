from src.kafka_consumer.json_consumer import consumer_using_sample_file

import os
if __name__=='__main__':
    topics = os.listdir("data")
    print(f'topics: [{topics}]')
    for topic in topics:
        sample_topic_data_dir = os.path.join("data",topic)
        sample_file_path = os.path.join(sample_topic_data_dir,os.listdir(sample_topic_data_dir)[0])
        consumer_using_sample_file(topic="kafka_sensor_topic",file_path = sample_file_path)