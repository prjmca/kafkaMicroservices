from time import sleep
from json import dumps
from kafka import KafkaProducer
import json
from kafka import KafkaProducer

# folderName=r"./"
# producer=KafkaProducer(
#     bootstrap_servers="kafka-2e0498be-pushpakpraneeth-a193.f.aivencloud.com:12381",
#     security_protocol="SSL",
#     ssl_cafile=r"ca.pem",
#     ssl_certfile=r"service.cert",
#     ssl_keyfile=r"service.key",
#     value_serializer=lambda v: json.dumps(v).encode('ascii'),
#     key_serializer=lambda v: json.dumps(v).encode('ascii')
# )

# producer.send("test-topis",key={'key':1},value={'message':'hello world'})
# producer.flush()

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)
# data1 = {"number": 600}
# data2 = {"number": 900}
# data3 = {"name": 'prudhvi'}
# data4 = {"firstname": 'pushpak'}
data5 = {"name": 'data11'}
data6 = {"firstname": 'data12'}
producer.send("newproducer3", value=data5)
producer.send("newproducer3", value=data6)

producer.flush()
# producer.send("numproduce2", value=data2)
# producer.send("numproduce3", value=data3)
# producer.send("numproduce4", value=data4)
print("sent", data5)
print("sent", data6)
# print("sent", data3)
# print("sent", data4)
