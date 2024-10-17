from confluent_kafka.admin import (AdminClient, NewTopic, ConfigResource)

config = {
     'bootstrap.servers': '172.18.0.11:9092,172.18.0.12:9092'
     }

def create_topicic(admin_client, topic_name="topicic", num_partitions=1, replication_factor=1):
    metadata = admin_client.list_topics()
    topics =  metadata.topics
    if topic_name not in topics:
        topic2 = NewTopic(topic_name, num_partitions, replication_factor)
        admin_client.create_topics([topic2])
        print('Topicic created')
    else:
        print('Topicic already exists')

if __name__ == '__main__':
    admin = AdminClient(config)
    create_topicic(admin, 'topicic', 5, 3)