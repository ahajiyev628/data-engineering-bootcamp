from confluent_kafka import Producer
# from config import config
import time

config = {
     'bootstrap.servers': '172.18.0.11:9092,172.18.0.12:9092'
     }

regions = {
    1: "Marmara",
    2: "Ege",
    3: "Akdeniz",
    4: "İç Anadolu"
}

def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        val = event.value().decode('utf-8')
        print(f'{val} sent to partition {event.partition()}.')

def send_region(producer, key, topic='regions'):
    value = regions[key]
    producer.produce(topic, value=value, key=str(key), on_delivery=callback)

if __name__ == '__main__':
    producer = Producer(config)
    topic = 'regions'

    for key in regions:
        send_region(producer=producer, key=key, topic=topic)
        time.sleep(5.0) 
        producer.flush()