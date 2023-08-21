from uuid import uuid4
from confluent_kafka import Producer

json_data= [{"name":"Gal", "email":"Gadot84@gmail.com", "salary": "8345.55"} ,{"name":"Dwayne", "email":"Johnson52@gmail.com", "salary": "7345.75"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"} ,{"name":"Momoa", "email":"Jason91@gmail.com", "salary": "3345.25"}]

kafka_topic_name = "sensor_topic"   

sasl_conf = {'sasl.mechanism': "PLAIN",
            'bootstrap.servers':"pkc-p11xm.us-east-1.aws.confluent.cloud:9092",
            'security.protocol': "SASL_SSL",
            'sasl.username': "5BFBCY3PZTC2ISMQ",
            'sasl.password': "sDYDXxGBdpHlSLzO2i2xLIhgAE6dT14GrDde3lJonifLauUcODjKRuDVB4o9rtq1"
            }


def delivery_report(err_msg,msg):
    if err_msg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), err_msg))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

producer1 = Producer(sasl_conf)
print(producer1)

producer1.poll(0.0)

for encode_data in json_data:
    json_string_encode=str(encode_data).encode()
    producer1.produce(topic=kafka_topic_name,key=str(uuid4()),value=json_string_encode,on_delivery=delivery_report)
    producer1.flush()

print("\stopping kafka cluster")