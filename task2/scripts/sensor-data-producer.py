#!/usr/bin/env python3
"""
Sensor Data Producer - Kafka Producer for Traffic Data Streaming
Simulates real-time streaming by publishing sensor readings using
timestamps from dataset to the 'traffic-raw' topic.
"""

import json
import time
import logging
import signal
import sys
from pathlib import Path
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SensorDataProducer:
    def __init__(self, kafka_bootstrap_servers, topic):
        """
        Initialize the Kafka producer for sensor data streaming.
        
        Args:
            kafka_bootstrap_servers (str): Kafka bootstrap servers
            topic (str): Target Kafka topic
        """
        self.topic = topic
        self.bootstrap_servers = kafka_bootstrap_servers
        self.producer = None
        self.running = False
        self.data_file = Path('/data/traffic.jsonl')
        
        # Handle graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self.running = False
    
    def _create_producer(self):
        """Create and configure Kafka producer."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                # Producer configuration for reliability
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,   # Retry failed sends
                batch_size=16384,  # Batch size in bytes
                linger_ms=10,      # Wait time before sending batch
                buffer_memory=33554432  # Total memory for buffering
            )
            logger.info(f"Kafka producer created successfully. Bootstrap servers: {self.bootstrap_servers}")
            return True
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            return False
    
    def _load_sensor_data(self):
        """Load sensor data from JSONL file."""
        try:
            if not self.data_file.exists():
                logger.error(f"Data file not found: {self.data_file}")
                return []
            
            sensor_data = []
            with open(self.data_file, 'r') as file:
                for line_num, line in enumerate(file, 1):
                    try:
                        record = json.loads(line.strip())
                        sensor_data.append(record)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON on line {line_num}: {e}")
                        continue
            
            logger.info(f"Loaded {len(sensor_data)} sensor records from {self.data_file}")
            return sensor_data
            
        except Exception as e:
            logger.error(f"Failed to load sensor data: {e}")
            return []
    
    def _send_record(self, record, record_index):
        """Send a single record to Kafka topic."""
        try:
            # Use atd_device_id as the key for partitioning
            key = record.get('atd_device_id', str(record_index))
            
            # Send the record
            future = self.producer.send(self.topic, key=key, value=record)
            
            # Add callback for success/failure
            future.add_callback(self._on_send_success, record_index, key)
            future.add_errback(self._on_send_error, record_index, key)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send record {record_index}: {e}")
            return False
    
    def _on_send_success(self, record_index, key, record_metadata):
        """Callback for successful message send."""
        logger.debug(f"Record {record_index} (key: {key}) sent successfully to "
                    f"topic: {record_metadata.topic}, partition: {record_metadata.partition}, "
                    f"offset: {record_metadata.offset}")
    
    def _on_send_error(self, record_index, key, exception):
        """Callback for failed message send."""
        logger.error(f"Failed to send record {record_index} (key: {key}): {exception}")
    
    def start_streaming(self):
        """
        Start streaming sensor data to Kafka topic with simple 1-second pause between records.
        """
        logger.info("Starting sensor data producer...")
        
        # Create Kafka producer
        if not self._create_producer():
            logger.error("Failed to create Kafka producer. Exiting.")
            return False
        
        # Load sensor data
        sensor_data = self._load_sensor_data()
        if not sensor_data:
            logger.error("No sensor data available. Exiting.")
            return False
        
        # Start streaming
        self.running = True
        record_count = 0
        data_index = 0
        
        logger.info(f"Starting to stream {len(sensor_data)} records to topic '{self.topic}' "
                   f"with 1-second pause between records...")
        
        try:
            while self.running and data_index < len(sensor_data):
                current_record = sensor_data[data_index].copy()  # Create a copy to avoid modifying original
                
                # Add producer metadata
                current_record['producer_timestamp'] = int(time.time() * 1000)  # milliseconds
                current_record['stream_sequence'] = record_count
                
                # Add timestamp field with dataset timestamp in ISO format
                current_record['timestamp'] = current_record.get('read_date')
                
                # Send the record
                if self._send_record(current_record, record_count):
                    record_count += 1
                    
                    logger.info(f"Sent record #{record_count}: Time={current_record.get('read_date')}, "
                               f"Device ID={current_record.get('atd_device_id')}, "
                               f"Intersection={current_record.get('intersection_name')}, "
                               f"Direction={current_record.get('direction')}, "
                               f"Volume={current_record.get('volume')}")
                
                data_index += 1
                
                # Simple 1-second pause between records
                if self.running and data_index < len(sensor_data):
                    time.sleep(0.01)
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Stopping...")
        except Exception as e:
            logger.error(f"Unexpected error during streaming: {e}")
        finally:
            self._cleanup()
        
        logger.info(f"Sensor data producer stopped. Total records sent: {record_count}")
        return True
    
    def _cleanup(self):
        """Cleanup resources."""
        if self.producer:
            try:
                logger.info("Flushing remaining messages...")
                self.producer.flush(timeout=10)  # Wait up to 10 seconds for messages to be sent
                logger.info("Closing producer...")
                self.producer.close(timeout=10)
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")


def main():
    """Main function to run the sensor data producer."""
    logger.info("=== Sensor Data Producer Started ===")
    
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = 'kafka-broker:9092'
    TOPIC = 'traffic_raw'
    
    # Create and start producer
    producer = SensorDataProducer(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=TOPIC
    )
    
    try:
        success = producer.start_streaming()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()