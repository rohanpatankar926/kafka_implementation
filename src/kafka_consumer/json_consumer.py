from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import sys,os
sys.path.append(os.getcwd())
from src.entity.entity import Generic
from src.kafka_config import sasl_conf
from src.database.database import MongodbOps




def consumer_using_sample_file(topic,file_path):
    schema_str = Generic.get_schema_to_produce_consume_data(filepath=file_path)
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Generic.dict_to_obj)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id': 'group1',
        'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    mongodb = MongodbOps()
    records = []
    x = 0
    while True:
        
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            record: Generic = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if record is not None:
                records.append(record.to_dict())
                if x % 5000 == 0:
                    mongodb.insert_many(collection_name="sensor_data", records=records)
                    records = []
            x = x + 1
            print(x,"records collected")
        except KeyboardInterrupt:
            break

    consumer.close()

