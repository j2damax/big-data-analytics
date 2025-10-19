#!/usr/bin/env python3
"""
Kafka Producer and Consumer Example
This script demonstrates basic Kafka operations using kafka-python
"""

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time
from datetime import datetime


class KafkaExample:
    """
    Demonstrates Kafka Producer and Consumer operations
    """
    
    def __init__(self, bootstrap_servers='kafka:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = 'bigdata-demo'
    
    def create_topic(self):
        """
        Create a Kafka topic if it doesn't exist
        """
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='topic-creator'
            )
            
            topic = NewTopic(
                name=self.topic_name,
                num_partitions=1,
                replication_factor=1
            )
            
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"Topic '{self.topic_name}' created successfully")
            admin_client.close()
        except Exception as e:
            print(f"Topic creation info: {e}")
    
    def produce_messages(self, num_messages=10):
        """
        Produce messages to Kafka topic
        
        Args:
            num_messages: Number of messages to produce
        """
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        print(f"\n=== Producing {num_messages} messages to topic '{self.topic_name}' ===")
        
        for i in range(num_messages):
            message = {
                'message_id': i,
                'timestamp': datetime.now().isoformat(),
                'technology': 'Kafka',
                'data': f'Sample data point {i}'
            }
            
            producer.send(self.topic_name, value=message)
            print(f"Sent: {message}")
            time.sleep(0.5)
        
        producer.flush()
        producer.close()
        print(f"\n{num_messages} messages produced successfully!")
    
    def consume_messages(self, timeout_ms=10000):
        """
        Consume messages from Kafka topic
        
        Args:
            timeout_ms: Timeout for consuming messages
        """
        consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='bigdata-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=timeout_ms
        )
        
        print(f"\n=== Consuming messages from topic '{self.topic_name}' ===")
        
        message_count = 0
        for message in consumer:
            print(f"Received: {message.value}")
            message_count += 1
        
        consumer.close()
        print(f"\n{message_count} messages consumed successfully!")


def main():
    """
    Main function to demonstrate Kafka operations
    """
    print("Starting Kafka Example...")
    print("Waiting for Kafka to be ready...")
    time.sleep(5)  # Wait for Kafka to be ready
    
    kafka_example = KafkaExample()
    
    # Create topic
    kafka_example.create_topic()
    time.sleep(2)
    
    # Produce messages
    kafka_example.produce_messages(num_messages=5)
    time.sleep(2)
    
    # Consume messages
    kafka_example.consume_messages(timeout_ms=10000)
    
    print("\nKafka Example completed!")


if __name__ == "__main__":
    main()
