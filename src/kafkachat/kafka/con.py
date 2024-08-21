from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
        "topic1",
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8')), #json loads(load) 가져옴/ 직렬화 역 직렬화/ 바이너리를 다시 역으로
        #consumer_timeout_ms=5000 #m/s단위
)

print('[Start] get consumer')

for msg in consumer:
    print(msg)

print('[End] get consumer')
