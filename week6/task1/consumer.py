from confluent_kafka import Consumer, KafkaException, KafkaError
# from config import config

config = {
     'bootstrap.servers': '172.18.0.11:9092,172.18.0.12:9092'
     }

def set_consumer_configs():
    config['group.id'] = 'task1_group'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False

def assignment_callback(consumer, partitions):
    for p in partitions:
        print(f'Assigned to {p.topic}, partition {p.partition}')

def consume_regions():
    set_consumer_configs()
    consumer = Consumer(config)
    consumer.subscribe(['regions'], on_assign=assignment_callback)

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
                timestamp = msg.timestamp()[1]

                print(f"Key: {key}, Value: {value}, Partition: {partition}, TS: {timestamp}")

    except KeyboardInterrupt:
        print("Consuming interrupted by user.")
    finally:
        consumer.close()

if __name__ == '__main__':
    consume_regions()
