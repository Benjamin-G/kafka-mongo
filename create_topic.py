import argparse
from kafka import KafkaClient

parser = argparse.ArgumentParser(description='Add topic to local Kafka Cluster  with localhost:9092 as bootstrap server')
parser.add_argument("-t", "--topic", type=str, required=True, help='name for topic')

args = parser.parse_args()
topic = args.topic
print(f"Creating {topic} Topic")

client = KafkaClient(
    bootstrap_servers=['localhost:9092']
)
client.add_topic(topic)

print("Success")