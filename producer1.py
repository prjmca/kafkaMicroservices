import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))
data = {'producer1' : "pushpak"}
producer.send('numtest12', value=data)
print('sent',data)
