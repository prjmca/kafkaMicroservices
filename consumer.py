from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    "testmicroservice1",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

for message in consumer:
    print(message)
