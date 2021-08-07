import argparse
from time import sleep
from json import dumps
from kafka import KafkaProducer

parser = argparse.ArgumentParser(description='Add a number of integers to local Kafka Cluster with localhost:9092 as bootstrap server')
parser.add_argument("-t", "--topic", type=str, required=True, help='name for topic')
parser.add_argument("-n", "--number", type=int, required=True, help='number of messages')
parser.add_argument("-s", "--sleep", type=int, help='number of messages', default=0)

args = parser.parse_args()
topic = args.topic
n = args.number
s = args.sleep


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

for e in range(n):
    data = {'number' : e}
    producer.send(topic, value=data)
    sleep(s)