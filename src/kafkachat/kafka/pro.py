from kafka import KafkaProducer
import time
import json
from tqdm import tqdm

producer = KafkaProducer(
        #bootstrap_servers=['localhost:9092'],
        #bootstrap_servers=['ec2-15-165-19-52.ap-northeast-2.compute.amazonaws.com:9092'],
        bootstrap_servers=['172.17.0.1:9092'],
        value_serializer=lambda x:json.dumps(x).encode('utf-8'), # 제이슨 덤프(직렬화) 엔코딩 한글
        compression_type='gzip',
        batch_size=100
)

start = time.time()

for i in tqdm(range(10000)):
    data = {'str': 'value' + str(i)}
    producer.send('test-gzip-100', value=data)
    producer.flush()
    time.sleep(0.001)

end = time.time()

print("[DONE]:", end - start)
