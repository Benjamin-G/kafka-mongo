import argparse
from kafka import KafkaConsumer
# TODO insert into Mongo DB
# from pymongo import MongoClient
from json import loads

parser = argparse.ArgumentParser(description='Consume (print) a topic from local Kafka Cluster with localhost:9092 as bootstrap server')
parser.add_argument("-t", "--topic", type=str, required=True, help='name for topic')

consumer = KafkaConsumer(
    'test_docker',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     consumer_timeout_ms=1000,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for m in consumer:
    print(f"{m}")    
    print(f"{m.value}")