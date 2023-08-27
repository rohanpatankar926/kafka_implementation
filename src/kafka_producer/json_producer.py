from uuid import uuid4
import sys,os
sys.path.append(os.getcwd())
from src.kafka_config import sasl_conf, schema_registry_config

import logging
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import pandas as pd
from src.entity.entity import Generic,instance_to_dict

FILE_PATH="data/kafka_sensor_topic/data.csv"

def delivery_report(err,msg):
    if err is not None:
        logging.info(f"Delivery of the message is unsuccessful {msg.key()} {err}")
        return
    logging.info(f"User record {msg.key()} {msg.topic()} {msg.partition()} {msg.offset()}")

def produce_data_using_file(topic,filepath):
    schema_str=Generic.get_schema_to_produce_consume_data(filepath=filepath)
    print(type(schema_str))
    input()
    schema_registry_config_=schema_registry_config()
    schema_registry_client=SchemaRegistryClient(schema_registry_config_)
    string_seriallizer=StringSerializer("utf_8")
    json_seriallizer=JSONSerializer(schema_str=schema_str,schema_registry_client=schema_registry_client,to_dict=instance_to_dict)
    producer=Producer(sasl_conf())
    producer.poll(0.0)
    for instance in Generic.get_object(file_path=filepath):
        print(instance.to_dict())
        producer.produce(topic=topic,
                             key=string_seriallizer(str(uuid4()), instance.to_dict()),
                             value=json_seriallizer(instance, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
        producer.flush()
