#!/usr/bin/env python3
"""
Kafka Temperature Sensor Producer

Educational Focus:
- Topic auto-creation with proper replication and partitioning
- Message serialization and partition assignment strategies
- Producer configuration and performance tuning
- Error handling and retry mechanisms
- Custom metrics emission for monitoring

This producer simulates IoT temperature sensors sending data to Kafka.
It demonstrates key concepts like partition distribution, serialization,
and producer acknowledgment patterns.
"""

import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from uuid import uuid4
import sys

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
import click
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge, start_http_server


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Prometheus metrics
MESSAGES_SENT = Counter('kafka_producer_messages_sent_total', 'Total messages sent', ['topic', 'partition'])
SEND_DURATION = Histogram('kafka_producer_send_duration_seconds', 'Message send duration')
PRODUCER_ERRORS = Counter('kafka_producer_errors_total', 'Total producer errors', ['error_type'])
ACTIVE_CONNECTIONS = Gauge('kafka_producer_connections_active', 'Active producer connections')


class SensorReading(BaseModel):
    """Data model for temperature sensor readings with validation."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    temperature: float = Field(ge=-50.0, le=100.0)  # Realistic temperature range
    sensor_id: str
    location: Optional[str] = None
    humidity: Optional[float] = Field(None, ge=0.0, le=100.0)

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes for Kafka."""
        return json.dumps(self.dict()).encode('utf-8')


class TemperatureProducer:
    """
    Temperature sensor data producer with comprehensive error handling and monitoring.

    Educational Features:
    - Automatic topic creation with educational parameters
    - Partition assignment demonstration
    - Producer configuration best practices
    - Comprehensive error handling and retry logic
    - Metrics emission for monitoring
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "learning-data",
        num_sensors: int = 10
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.num_sensors = num_sensors
        self.producer: Optional[KafkaProducer] = None
        self.admin_client: Optional[KafkaAdminClient] = None
        self.messages_sent = 0
        self.last_log_time = time.time()

    def _create_admin_client(self) -> KafkaAdminClient:
        """Create admin client for topic management."""
        return KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id='temperature-producer-admin',
            request_timeout_ms=30000,
            api_version=(2, 5, 0)  # Kafka 3.9 compatible
        )

    def _create_producer(self) -> KafkaProducer:
        """
        Create Kafka producer with educational configuration.

        Configuration Highlights:
        - acks='all': Wait for all replicas to acknowledge (durability)
        - retries=10: Retry failed sends (reliability)
        - batch_size=16384: Optimize throughput with batching
        - linger_ms=10: Small delay to allow batching
        - compression_type='snappy': Efficient compression
        """
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id='temperature-producer',

            # Reliability settings
            acks='all',  # Wait for all in-sync replicas
            retries=10,  # Retry failed sends
            retry_backoff_ms=100,

            # Performance settings
            batch_size=16384,  # 16KB batch size
            linger_ms=10,      # Wait 10ms to form batches
            buffer_memory=33554432,  # 32MB buffer

            # Compression for efficiency
            compression_type='snappy',

            # Serialization
            key_serializer=str.encode,
            value_serializer=lambda v: v,  # Already serialized in SensorReading

            # Timeouts
            request_timeout_ms=30000,
            delivery_timeout_ms=120000,

            # API version
            api_version=(2, 5, 0)
        )

    def ensure_topic_exists(self) -> None:
        """
        Create topic if it doesn't exist with educational parameters.

        Topic Configuration:
        - 6 partitions: Allows parallel processing and demonstrates partitioning
        - Replication factor 2: Fault tolerance without over-replication
        - Retention 7 days: Reasonable for learning scenarios
        """
        try:
            self.admin_client = self._create_admin_client()

            topic_config = {
                'cleanup.policy': 'delete',
                'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
                'segment.ms': str(24 * 60 * 60 * 1000),        # 1 day segments
                'compression.type': 'producer'                   # Use producer compression
            }

            new_topic = NewTopic(
                name=self.topic,
                num_partitions=6,      # Educational: allows partition demonstration
                replication_factor=2,  # Educational: fault tolerance
                topic_configs=topic_config
            )

            self.admin_client.create_topics([new_topic], validate_only=False)
            logger.info(f"Created topic '{self.topic}' with 6 partitions and replication factor 2")

        except TopicAlreadyExistsError:
            logger.info(f"Topic '{self.topic}' already exists")
        except Exception as e:
            logger.error(f"Failed to create topic: {e}")
            PRODUCER_ERRORS.labels(error_type="topic_creation").inc()
            raise

    def generate_sensor_reading(self) -> SensorReading:
        """Generate a realistic temperature sensor reading."""
        return SensorReading(
            temperature=round(random.uniform(15.0, 35.0), 2),
            sensor_id=f"sensor_{random.randint(1, self.num_sensors)}",
            location=random.choice([
                "server_room", "warehouse", "office_floor_1",
                "office_floor_2", "parking_garage", "roof_deck"
            ]),
            humidity=round(random.uniform(30.0, 80.0), 2)
        )

    def _delivery_callback(self, record_metadata, exception):
        """
        Callback for message delivery confirmation.

        Educational: Shows how to handle async delivery confirmations
        and track success/failure rates for monitoring.
        """
        if exception:
            logger.error(f"Message delivery failed: {exception}")
            PRODUCER_ERRORS.labels(error_type="delivery_failure").inc()
        else:
            # Update metrics
            MESSAGES_SENT.labels(
                topic=record_metadata.topic,
                partition=str(record_metadata.partition)
            ).inc()

            self.messages_sent += 1

            # Log progress every 100 messages
            if self.messages_sent % 100 == 0:
                current_time = time.time()
                rate = 100 / (current_time - self.last_log_time)
                logger.info(
                    f"Sent {self.messages_sent} messages. "
                    f"Last 100 messages: {rate:.1f} msg/sec. "
                    f"Latest: topic={record_metadata.topic}, "
                    f"partition={record_metadata.partition}, "
                    f"offset={record_metadata.offset}"
                )
                self.last_log_time = current_time

    def start_producing(self, rate_limit: float = 10.0, duration: Optional[int] = None) -> None:
        """
        Start producing temperature data.

        Args:
            rate_limit: Messages per second (allows burst but averages to this rate)
            duration: Optional duration in seconds (infinite if None)
        """
        try:
            # Initialize components
            self.ensure_topic_exists()
            self.producer = self._create_producer()
            ACTIVE_CONNECTIONS.set(1)

            logger.info(f"Starting temperature data production at {rate_limit} msg/sec")
            if duration:
                logger.info(f"Will run for {duration} seconds")

            start_time = time.time()
            message_interval = 1.0 / rate_limit
            next_send_time = start_time

            while True:
                # Check duration limit
                if duration and (time.time() - start_time) >= duration:
                    logger.info(f"Reached duration limit of {duration} seconds")
                    break

                # Rate limiting
                current_time = time.time()
                if current_time < next_send_time:
                    time.sleep(next_send_time - current_time)

                # Generate and send message
                with SEND_DURATION.time():
                    reading = self.generate_sensor_reading()

                    # Use sensor_id as key for consistent partitioning
                    # This ensures all data from the same sensor goes to the same partition
                    future = self.producer.send(
                        topic=self.topic,
                        key=reading.sensor_id,
                        value=reading.to_json_bytes()
                    )

                    # Add async callback for delivery confirmation
                    future.add_callback(self._delivery_callback)
                    future.add_errback(lambda ex: self._delivery_callback(None, ex))

                next_send_time = current_time + message_interval

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            PRODUCER_ERRORS.labels(error_type="kafka_error").inc()
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            PRODUCER_ERRORS.labels(error_type="unexpected_error").inc()
            raise
        finally:
            self._shutdown()

    def _shutdown(self) -> None:
        """Clean shutdown of producer and admin client."""
        if self.producer:
            logger.info("Flushing remaining messages...")
            self.producer.flush(timeout=10)
            self.producer.close()
            ACTIVE_CONNECTIONS.set(0)

        if self.admin_client:
            self.admin_client.close()

        logger.info(f"Producer shutdown complete. Total messages sent: {self.messages_sent}")


@click.command()
@click.option(
    '--bootstrap-servers',
    default='localhost:9092',
    help='Kafka bootstrap servers',
    show_default=True
)
@click.option(
    '--topic',
    default='learning-data',
    help='Kafka topic name',
    show_default=True
)
@click.option(
    '--rate',
    default=10.0,
    type=float,
    help='Messages per second',
    show_default=True
)
@click.option(
    '--duration',
    type=int,
    help='Duration in seconds (infinite if not specified)'
)
@click.option(
    '--num-sensors',
    default=10,
    type=int,
    help='Number of simulated sensors',
    show_default=True
)
@click.option(
    '--metrics-port',
    default=8000,
    type=int,
    help='Prometheus metrics port',
    show_default=True
)
def main(
    bootstrap_servers: str,
    topic: str,
    rate: float,
    duration: Optional[int],
    num_sensors: int,
    metrics_port: int
):
    """
    Temperature sensor data producer for Kafka learning.

    This producer demonstrates key Kafka concepts:
    - Automatic topic creation with proper configuration
    - Partition key usage for data locality
    - Producer configuration for reliability and performance
    - Error handling and monitoring
    - Custom metrics emission

    Example usage:
        python producer.py --rate 50 --duration 300
        python producer.py --bootstrap-servers kafka:9092 --topic sensors
    """

    # Start Prometheus metrics server
    try:
        start_http_server(metrics_port)
        logger.info(f"Prometheus metrics available at http://localhost:{metrics_port}/metrics")
    except Exception as e:
        logger.warning(f"Failed to start metrics server: {e}")

    # Create and start producer
    producer = TemperatureProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        num_sensors=num_sensors
    )

    producer.start_producing(rate_limit=rate, duration=duration)


if __name__ == '__main__':
    main()