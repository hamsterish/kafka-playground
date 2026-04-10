#!/usr/bin/env python3
"""
Kafka Temperature Data Consumer with Windowed Aggregation

Educational Focus:
- Consumer group management and partition assignment
- Offset management and commit strategies
- Windowed data processing and aggregation
- Consumer lag monitoring and alerting
- Graceful shutdown and error handling

This consumer demonstrates real-time data processing patterns
commonly used in streaming applications.
"""

import json
import logging
import signal
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Deque
import statistics

from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from kafka.errors import CommitFailedError, KafkaError
import click
from pydantic import BaseModel, ValidationError
from prometheus_client import Counter, Histogram, Gauge, start_http_server


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Prometheus metrics
MESSAGES_CONSUMED = Counter('kafka_consumer_messages_consumed_total', 'Total messages consumed', ['topic', 'partition'])
PROCESSING_DURATION = Histogram('kafka_consumer_processing_duration_seconds', 'Message processing duration')
CONSUMER_LAG = Gauge('kafka_consumer_lag_total', 'Consumer lag', ['topic', 'partition', 'consumer_group'])
CONSUMER_ERRORS = Counter('kafka_consumer_errors_total', 'Total consumer errors', ['error_type'])
ACTIVE_PARTITIONS = Gauge('kafka_consumer_partitions_active', 'Active partitions assigned to consumer')
WINDOW_CALCULATIONS = Counter('kafka_consumer_window_calculations_total', 'Total window calculations performed')


class SensorReading(BaseModel):
    """Data model matching the producer's sensor readings."""
    id: str
    timestamp: str
    temperature: float
    sensor_id: str
    location: Optional[str] = None
    humidity: Optional[float] = None

    @classmethod
    def from_json_bytes(cls, data: bytes) -> 'SensorReading':
        """Deserialize from JSON bytes."""
        return cls(**json.loads(data.decode('utf-8')))


class WindowedAggregator:
    """
    Time-based windowed aggregation for temperature data.

    Educational Features:
    - Rolling window calculations
    - Per-sensor data tracking
    - Statistical aggregations (mean, min, max, std)
    - Window expiration and cleanup
    """

    def __init__(self, window_size_seconds: int = 60):
        self.window_size = window_size_seconds
        # Structure: sensor_id -> deque of (timestamp, temperature) tuples
        self.sensor_data: Dict[str, Deque[tuple]] = defaultdict(lambda: deque())
        self.last_calculation_time = time.time()

    def add_reading(self, reading: SensorReading) -> None:
        """Add a sensor reading to the window."""
        timestamp = datetime.fromisoformat(reading.timestamp.replace('Z', '+00:00'))
        timestamp_seconds = timestamp.timestamp()

        self.sensor_data[reading.sensor_id].append((timestamp_seconds, reading.temperature))
        self._cleanup_old_data()

    def _cleanup_old_data(self) -> None:
        """Remove data older than the window size."""
        current_time = time.time()
        cutoff_time = current_time - self.window_size

        for sensor_id, data_deque in self.sensor_data.items():
            while data_deque and data_deque[0][0] < cutoff_time:
                data_deque.popleft()

    def calculate_aggregations(self) -> Dict[str, Dict[str, float]]:
        """
        Calculate windowed aggregations for all sensors.

        Returns:
            Dictionary with sensor_id as key and aggregation stats as value.
        """
        results = {}
        current_time = time.time()

        for sensor_id, data_deque in self.sensor_data.items():
            if not data_deque:
                continue

            temperatures = [temp for _, temp in data_deque]

            if temperatures:
                try:
                    results[sensor_id] = {
                        'count': len(temperatures),
                        'mean': round(statistics.mean(temperatures), 2),
                        'min': round(min(temperatures), 2),
                        'max': round(max(temperatures), 2),
                        'std': round(statistics.stdev(temperatures), 2) if len(temperatures) > 1 else 0.0,
                        'window_start': min(timestamp for timestamp, _ in data_deque),
                        'window_end': max(timestamp for timestamp, _ in data_deque),
                        'calculation_time': current_time
                    }
                except statistics.StatisticsError as e:
                    logger.warning(f"Statistics calculation failed for sensor {sensor_id}: {e}")
                    CONSUMER_ERRORS.labels(error_type="statistics_error").inc()

        WINDOW_CALCULATIONS.inc()
        return results


class TemperatureConsumer:
    """
    Temperature data consumer with windowed aggregation and comprehensive monitoring.

    Educational Features:
    - Consumer group membership and partition rebalancing
    - Manual offset management with periodic commits
    - Lag monitoring and alerting
    - Graceful shutdown handling
    - Window-based real-time processing
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "learning-data",
        consumer_group: str = "temperature-aggregator",
        window_size: int = 60
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer_group = consumer_group
        self.consumer: Optional[KafkaConsumer] = None
        self.aggregator = WindowedAggregator(window_size)
        self.running = True
        self.last_commit_time = time.time()
        self.commit_interval = 5.0  # Commit every 5 seconds
        self.last_stats_time = time.time()
        self.stats_interval = 10.0  # Print stats every 10 seconds
        self.messages_processed = 0

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False

    def _create_consumer(self) -> KafkaConsumer:
        """
        Create Kafka consumer with educational configuration.

        Configuration Highlights:
        - auto_offset_reset='earliest': Start from beginning if no offset stored
        - enable_auto_commit=False: Manual offset management for learning
        - max_poll_records=100: Process in small batches for responsiveness
        - session_timeout_ms=30000: Reasonable timeout for learning environment
        """
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            client_id='temperature-consumer',
            group_id=self.consumer_group,

            # Offset management
            auto_offset_reset='earliest',    # Start from beginning if no stored offset
            enable_auto_commit=False,        # Manual commit for educational purposes

            # Performance settings
            max_poll_records=100,           # Process in small batches
            fetch_min_bytes=1024,           # Minimum 1KB fetch
            fetch_max_wait_ms=500,          # Wait up to 500ms for fetch_min_bytes

            # Session management
            session_timeout_ms=30000,       # 30 second session timeout
            heartbeat_interval_ms=10000,    # Heartbeat every 10 seconds

            # Deserialization
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            value_deserializer=lambda v: v,  # Handle in application code

            # API version
            api_version=(2, 5, 0)
        )

    def _update_consumer_lag(self):
        """Update consumer lag metrics for monitoring."""
        try:
            # Get current partition assignments
            partitions = self.consumer.assignment()

            for partition in partitions:
                # Get high water mark (latest offset)
                high_water_mark = self.consumer.highwater(partition)

                # Get current consumer position
                position = self.consumer.position(partition)

                if high_water_mark is not None and position is not None:
                    lag = high_water_mark - position
                    CONSUMER_LAG.labels(
                        topic=partition.topic,
                        partition=str(partition.partition),
                        consumer_group=self.consumer_group
                    ).set(lag)

                    if lag > 1000:  # Alert on high lag
                        logger.warning(f"High consumer lag detected: {lag} messages on {partition}")

        except Exception as e:
            logger.warning(f"Failed to update consumer lag metrics: {e}")
            CONSUMER_ERRORS.labels(error_type="lag_monitoring").inc()

    def _commit_offsets(self, force: bool = False):
        """Commit consumer offsets periodically or on demand."""
        current_time = time.time()

        if force or (current_time - self.last_commit_time) >= self.commit_interval:
            try:
                self.consumer.commit()
                self.last_commit_time = current_time
                logger.debug("Offsets committed successfully")
            except CommitFailedError as e:
                logger.error(f"Failed to commit offsets: {e}")
                CONSUMER_ERRORS.labels(error_type="commit_failure").inc()

    def _print_statistics(self, force: bool = False):
        """Print processing statistics periodically."""
        current_time = time.time()

        if force or (current_time - self.last_stats_time) >= self.stats_interval:
            # Calculate aggregations
            aggregations = self.aggregator.calculate_aggregations()

            if aggregations:
                logger.info(f"=== Temperature Aggregations (last {self.aggregator.window_size}s) ===")
                for sensor_id, stats in aggregations.items():
                    logger.info(
                        f"Sensor {sensor_id}: "
                        f"count={stats['count']}, "
                        f"mean={stats['mean']}°C, "
                        f"min={stats['min']}°C, "
                        f"max={stats['max']}°C, "
                        f"std={stats['std']}°C"
                    )
                logger.info(f"Total messages processed: {self.messages_processed}")
            else:
                logger.info("No sensor data in current window")

            self.last_stats_time = current_time

    def start_consuming(self) -> None:
        """
        Start consuming temperature data with windowed aggregation.

        Processing Flow:
        1. Poll for messages
        2. Deserialize and validate each message
        3. Add to windowed aggregator
        4. Update metrics and lag monitoring
        5. Periodically commit offsets and print stats
        """
        try:
            self.consumer = self._create_consumer()
            logger.info(f"Starting consumer for topic '{self.topic}' in group '{self.consumer_group}'")

            # Initial partition assignment and lag measurement
            self.consumer.poll(timeout_ms=1000, max_records=0)  # Trigger partition assignment
            assigned_partitions = self.consumer.assignment()
            ACTIVE_PARTITIONS.set(len(assigned_partitions))
            logger.info(f"Assigned partitions: {[f'{p.topic}-{p.partition}' for p in assigned_partitions]}")

            while self.running:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000, max_records=100)

                if not message_batch:
                    # No messages, but still perform housekeeping
                    self._update_consumer_lag()
                    self._commit_offsets()
                    self._print_statistics()
                    continue

                # Process messages
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            with PROCESSING_DURATION.time():
                                # Deserialize message
                                reading = SensorReading.from_json_bytes(message.value)

                                # Add to aggregator
                                self.aggregator.add_reading(reading)

                                # Update metrics
                                MESSAGES_CONSUMED.labels(
                                    topic=message.topic,
                                    partition=str(message.partition)
                                ).inc()

                                self.messages_processed += 1

                        except ValidationError as e:
                            logger.warning(f"Invalid message format: {e}")
                            CONSUMER_ERRORS.labels(error_type="validation_error").inc()
                        except json.JSONDecodeError as e:
                            logger.warning(f"JSON decode error: {e}")
                            CONSUMER_ERRORS.labels(error_type="json_decode_error").inc()
                        except Exception as e:
                            logger.error(f"Unexpected error processing message: {e}")
                            CONSUMER_ERRORS.labels(error_type="processing_error").inc()

                # Periodic housekeeping
                self._update_consumer_lag()
                self._commit_offsets()
                self._print_statistics()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            CONSUMER_ERRORS.labels(error_type="kafka_error").inc()
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            CONSUMER_ERRORS.labels(error_type="unexpected_error").inc()
            raise
        finally:
            self._shutdown()

    def _shutdown(self) -> None:
        """Clean shutdown of consumer."""
        logger.info("Shutting down consumer...")

        if self.consumer:
            try:
                # Final statistics
                self._print_statistics(force=True)

                # Final offset commit
                self._commit_offsets(force=True)

                # Close consumer
                self.consumer.close()
                ACTIVE_PARTITIONS.set(0)

            except Exception as e:
                logger.error(f"Error during consumer shutdown: {e}")

        logger.info(f"Consumer shutdown complete. Total messages processed: {self.messages_processed}")


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
    '--consumer-group',
    default='temperature-aggregator',
    help='Consumer group ID',
    show_default=True
)
@click.option(
    '--window-size',
    default=60,
    type=int,
    help='Aggregation window size in seconds',
    show_default=True
)
@click.option(
    '--metrics-port',
    default=8001,
    type=int,
    help='Prometheus metrics port',
    show_default=True
)
def main(
    bootstrap_servers: str,
    topic: str,
    consumer_group: str,
    window_size: int,
    metrics_port: int
):
    """
    Temperature data consumer with windowed aggregation.

    This consumer demonstrates key Kafka concepts:
    - Consumer group membership and partition assignment
    - Manual offset management and commit strategies
    - Real-time windowed data processing
    - Consumer lag monitoring and alerting
    - Graceful shutdown handling

    The consumer calculates rolling statistics for temperature sensors
    and prints aggregated results every 10 seconds.

    Example usage:
        python consumer.py --window-size 120 --consumer-group analytics
        python consumer.py --bootstrap-servers kafka:9092 --topic sensors
    """

    # Start Prometheus metrics server
    try:
        start_http_server(metrics_port)
        logger.info(f"Prometheus metrics available at http://localhost:{metrics_port}/metrics")
    except Exception as e:
        logger.warning(f"Failed to start metrics server: {e}")

    # Create and start consumer
    consumer = TemperatureConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        consumer_group=consumer_group,
        window_size=window_size
    )

    consumer.start_consuming()


if __name__ == '__main__':
    main()