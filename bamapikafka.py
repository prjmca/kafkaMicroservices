import json
from confluent_kafka import Producer
from confluent_kafka import Consumer,KafkaError
import requests

def getdata(endpoint):
    response =  requests.get(endpoint, verify=False)
    return response.json()

def senddata(producer, topic, data):
    producer.send(topic, value=data)
    producer.flush()

producer = Producer({
    'bootstrap.servers':"localhost:9092",
    # topic = 'microservice1',
})

consumer = Consumer({
    'bootstrap.servers':"localhost:9092",
    'auto.offset.reset':"earliest",
    'enable.auto.commit':True,
    'group.id' : '12345'
})

endpoints1 = ['https://bam-referencedata-service.dit.us.caas.oneadp.com/api/ReferenceData/GetGeoStates?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXIiOiJzeXN0ZW0iLCJyZWdpb24iOiJBTEwiLCJhcHAiOiJTeXN0ZW0ifSwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjI1NTQzNTA4MzB9.63Ahd1IW9Ry_n8-XjMM-GbmmDqZtwJy9cw7Jn6kftcw', 
              'https://bam-referencedata-service.dit.us.caas.oneadp.com/api/ReferenceData/GetAscertainReasons?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXIiOiJzeXN0ZW0iLCJyZWdpb24iOiJBTEwiLCJhcHAiOiJTeXN0ZW0ifSwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjI1NTQzNTA4MzB9.63Ahd1IW9Ry_n8-XjMM-GbmmDqZtwJy9cw7Jn6kftcw',
              'https://bam-referencedata-service.dit.us.caas.oneadp.com/api/ReferenceData/GetBatchTypes?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXIiOiJzeXN0ZW0iLCJyZWdpb24iOiJBTEwiLCJhcHAiOiJTeXN0ZW0ifSwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjI1NTQzNTA4MzB9.63Ahd1IW9Ry_n8-XjMM-GbmmDqZtwJy9cw7Jn6kftcw']
endpoints2 = ['https://bam-referencedata-service.dit.us.caas.oneadp.com/api/ReferenceData/GetCatalogItems?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXIiOiJzeXN0ZW0iLCJyZWdpb24iOiJBTEwiLCJhcHAiOiJTeXN0ZW0ifSwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjI1NTQzNTA4MzB9.63Ahd1IW9Ry_n8-XjMM-GbmmDqZtwJy9cw7Jn6kftcw',
               'https://bam-referencedata-service.dit.us.caas.oneadp.com/api/ReferenceData/GetDeliveryMethod?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXIiOiJzeXN0ZW0iLCJyZWdpb24iOiJBTEwiLCJhcHAiOiJTeXN0ZW0ifSwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjI1NTQzNTA4MzB9.63Ahd1IW9Ry_n8-XjMM-GbmmDqZtwJy9cw7Jn6kftcw']
topic = 'microservice1'

for endpoint in endpoints1:
    # datams = getdata(endpoint)
    # senddata(producer, 'testmicroservice1', datams)
    producer.produce(topic, key='microservice_key', value=endpoint.encode('utf-8'))
    producer.flush()
    #print("sent", datams)

consumer.subscribe([topic])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition, continue polling
            continue
        else:
            print(f"Consumer error: {msg.error()}")
            break

    # Extract the microservice endpoint from the message
    microservice_endpoint = msg.value().decode('utf-8')

    # Make an HTTP GET request to the microservice endpoint
    try:
        response = requests.get(microservice_endpoint,verify=False)
        if response.status_code == 200:
            print(f"Received response from {microservice_endpoint}: {response.text}")
        else:
            print(f"Error: Failed to get response from {microservice_endpoint}. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error: {e}")

# Close consumer
consumer.close() 
