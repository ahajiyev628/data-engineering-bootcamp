from confluent_kafka import Consumer, KafkaException, KafkaError
import os
import time
# from config import config

config = {
     'bootstrap.servers': '172.18.0.11:9092,172.18.0.12:9092'
     }

OUTPUT_DIR = '/tmp/kafka_out'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def set_consumer_configs():
    config['group.id'] = 'iris_group'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False

def assignment_callback(consumer, partitions):
    for p in partitions:
        print(f'Assigned to {p.topic}, partition {p.partition}')


def write_to_file(topic, partition, offset, key, message_value):
    species = message_value.split(',')[-1]
    species = species.replace('Iris-','')
    
    if species not in ['setosa', 'versicolor','virginica']:
        species = 'other'
    
    print(species)
    file_name = f'{species.lower()}_out.txt'
    print("file_name")
    print(file_name)
    file_path = os.path.join(OUTPUT_DIR, file_name)
    
    log_entry = f"{topic}|{partition}|{offset}|{key}|{message_value}\n"
    print(log_entry)
    
    with open(file_path, 'a') as f:
        f.write(log_entry)

def consume_iris_data():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition()}")
                else:
                    raise KafkaException(msg.error())
            else:
                key = msg.key().decode('utf-8')
                value = msg.value().decode('utf-8')
                partition = msg.partition()
                offset = msg.offset()

                write_to_file(msg.topic(), partition, offset, key, value)

                print(f"Message from partition {partition} with offset {offset} written to file.")

    except KeyboardInterrupt:
        print("Consuming interrupted by user.")
    finally:
        consumer.close()

if __name__ == "__main__":
    set_consumer_configs()
    consumer = Consumer(config)
    consumer.subscribe(['topic1'], on_assign=assignment_callback)
    consume_iris_data()
