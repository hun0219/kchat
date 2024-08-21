from kafka import KafkaProducer
import time
import json
from tqdm import tqdm

producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x:json.dumps(x).encode('utf-8') # 제이슨 덤프(직렬화) 엔코딩 한글
)

start = time.time()

for i in tqdm(range(10)):
    data = {'str': 'value' + str(i)}
    producer.send('topic1', value=data)
    producer.flush()
    time.sleep(0.1)

end = time.time()

print("[DONE]:", end - start)
