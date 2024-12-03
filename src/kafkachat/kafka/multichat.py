from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time
from datetime import datetime
import sys

# 처음 py실행시 창 클린
def clear_screen():
    sys.stdout.write("\033[H\033[J")

messages = []

def receiver(username):
    global messages
    consumer = KafkaConsumer(
    'chat',
    #bootstrap_servers=['ec2-15-165-19-52.ap-northeast-2.compute.amazonaws.com:9092'],
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    # group_id=f'chat-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    try:
        for m in consumer:
            data = m.value
            formatted_time = datetime.fromtimestamp(data['time']).strftime("%Y-%m-%d %H:%M:%S")
            #print(f"{data['name']}: {data['message']}  [{formatted_time}]")
            messages.append(f"{data['username']}: {data['message']} [{formatted_time}]")

            clear_screen()
    
            for message in messages:
                print(message)

            sys.stdout.write(f"{username}:")
            sys.stdout.flush()
    except KeyboardInterrupt:
        print("채팅 종료")
    finally:
        consumer.close()

def sender(username):
    producer = KafkaProducer(
    #bootstrap_servers=['ec2-15-165-19-52.ap-northeast-2.compute.amazonaws.com:9092'],
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    while True:

        #message = sys.stdout.write()
        #sys.stdout.flush()
        message = input()
        data = {'username':username, 'message': message, 'time': time.time()}

        if message == 'exit':
            producer.close()
            break

        producer.send('chat', value=data)
        producer.flush()

if __name__ == "__main__":
    print("채팅 프로그램 - 메시지 발신 및 수신")
    username = input("사용할 이름을 입력하세요 : ")

    consumer_thread = threading.Thread(target=receiver, args=(username,))
    producer_thread = threading.Thread(target=sender, args=(username,))

    consumer_thread.start()
    producer_thread.start()
