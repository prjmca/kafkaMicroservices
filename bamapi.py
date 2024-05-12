import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import requests

def getdata(endpoint):
    response =  requests.get(endpoint, verify=False)
    return response.json()

def senddata(producer, topic, data):
    producer.send(topic, value=data)
    producer.flush()

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

consumer = KafkaConsumer(
    "testmicroservice1",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

endpoints1 = ['https://bam-referencedata-service.dit.us.caas.oneadp.com/api/ReferenceData/GetGeoStates?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXIiOiJzeXN0ZW0iLCJyZWdpb24iOiJBTEwiLCJhcHAiOiJTeXN0ZW0ifSwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjI1NTQzNTA4MzB9.63Ahd1IW9Ry_n8-XjMM-GbmmDqZtwJy9cw7Jn6kftcw', 
              'https://bam-referencedata-service.dit.us.caas.oneadp.com/api/ReferenceData/GetAscertainReasons?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXIiOiJzeXN0ZW0iLCJyZWdpb24iOiJBTEwiLCJhcHAiOiJTeXN0ZW0ifSwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjI1NTQzNTA4MzB9.63Ahd1IW9Ry_n8-XjMM-GbmmDqZtwJy9cw7Jn6kftcw',
              'https://bam-referencedata-service.dit.us.caas.oneadp.com/api/ReferenceData/GetBatchTypes?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXIiOiJzeXN0ZW0iLCJyZWdpb24iOiJBTEwiLCJhcHAiOiJTeXN0ZW0ifSwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjI1NTQzNTA4MzB9.63Ahd1IW9Ry_n8-XjMM-GbmmDqZtwJy9cw7Jn6kftcw']
endpoints2 = ['https://bam-referencedata-service.dit.us.caas.oneadp.com/api/ReferenceData/GetCatalogItems?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXIiOiJzeXN0ZW0iLCJyZWdpb24iOiJBTEwiLCJhcHAiOiJTeXN0ZW0ifSwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjI1NTQzNTA4MzB9.63Ahd1IW9Ry_n8-XjMM-GbmmDqZtwJy9cw7Jn6kftcw',
               'https://bam-referencedata-service.dit.us.caas.oneadp.com/api/ReferenceData/GetDeliveryMethod?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXIiOiJzeXN0ZW0iLCJyZWdpb24iOiJBTEwiLCJhcHAiOiJTeXN0ZW0ifSwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjI1NTQzNTA4MzB9.63Ahd1IW9Ry_n8-XjMM-GbmmDqZtwJy9cw7Jn6kftcw']

for endpoint in endpoints1:
    datams = getdata(endpoint)
    senddata(producer, 'testmicroservice1', datams)
    #print("sent", datams)

# for endpoint in endpoints2:
#     datams = getdata(endpoint)
#     senddata(producer, 'testmicroservice2', datams)
#     print("sent", datams)

for message in consumer:
    print(message)
# dataAPI = getdata()
# data = {"number": 100}
# producer.send("testing", value=dataAPI)
# print("sent", dataAPI)