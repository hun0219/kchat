from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
        "topic1",
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8')), #json loads(load) 가져옴/ 직렬화 역 직렬화/ 바이너리를 다시 역으로
        consumer_timeout_ms=5000, #m/s단위
        #auto_offset_reset='earliest', # 'earliest'이미 있던 데이터 포함  'latest'나중에 온거
        auto_offset_reset='earliest', # 'earliest', 'latest'
        group_id="tving",
        enable_auto_commit=True,
)

print('[Start] get consumer')

for msg in consumer:
#    print(msg)
    print(f"topic={msg.topic}, partition={msg.partition}, offset={msg.offset}")
#    print(f"ConsumerRecord(topic='topic1', partition=0, offset=109, timestamp=1724218972238, timestamp_type=0, key=None, value={'str': 'value9'}, headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=17, serialized_header_size=-1)")

print('[End] get consumer')
