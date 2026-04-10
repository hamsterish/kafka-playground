"""
Unit Tests for Kafka Producer and Consumer Scripts

Educational Focus:
- Testing Kafka client applications without requiring a running cluster
- Mocking Kafka components for isolated testing
- Validating data serialization and business logic
- Testing error handling and edge cases

These tests demonstrate how to test Kafka applications in isolation,
which is crucial for reliable CI/CD pipelines.
"""

import json
import pytest
import time
from datetime import datetime, timezone
from unittest.mock import Mock, MagicMock, patch, call
from uuid import UUID

# Import the modules we're testing
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from producer import SensorReading, TemperatureProducer, WindowedAggregator
from consumer import SensorReading as ConsumerSensorReading, TemperatureConsumer


class TestSensorReading:
    """Test the SensorReading data model."""

    def test_sensor_reading_creation_with_defaults(self):
        """Test creating a sensor reading with automatic defaults."""
        reading = SensorReading(
            temperature=25.5,
            sensor_id="sensor_1"
        )

        assert reading.temperature == 25.5
        assert reading.sensor_id == "sensor_1"
        assert reading.location is None
        assert reading.humidity is None

        # Validate UUID format
        UUID(reading.id)  # This will raise ValueError if not valid UUID

        # Validate ISO timestamp format
        datetime.fromisoformat(reading.timestamp.replace('Z', '+00:00'))

    def test_sensor_reading_validation_temperature_range(self):
        """Test temperature validation boundaries."""
        # Valid temperatures
        SensorReading(temperature=-50.0, sensor_id="test")  # Minimum
        SensorReading(temperature=100.0, sensor_id="test")  # Maximum
        SensorReading(temperature=25.5, sensor_id="test")   # Normal

        # Invalid temperatures
        with pytest.raises(ValueError):
            SensorReading(temperature=-51.0, sensor_id="test")  # Below minimum

        with pytest.raises(ValueError):
            SensorReading(temperature=101.0, sensor_id="test")  # Above maximum

    def test_sensor_reading_validation_humidity_range(self):
        """Test humidity validation when provided."""
        # Valid humidity
        SensorReading(temperature=25.0, sensor_id="test", humidity=0.0)    # Minimum
        SensorReading(temperature=25.0, sensor_id="test", humidity=100.0)  # Maximum
        SensorReading(temperature=25.0, sensor_id="test", humidity=65.5)   # Normal

        # Invalid humidity
        with pytest.raises(ValueError):
            SensorReading(temperature=25.0, sensor_id="test", humidity=-1.0)  # Below minimum

        with pytest.raises(ValueError):
            SensorReading(temperature=25.0, sensor_id="test", humidity=101.0)  # Above maximum

    def test_sensor_reading_json_serialization(self):
        """Test JSON serialization round-trip."""
        reading = SensorReading(
            temperature=23.7,
            sensor_id="sensor_42",
            location="server_room",
            humidity=45.2
        )

        # Serialize to JSON bytes
        json_bytes = reading.to_json_bytes()
        assert isinstance(json_bytes, bytes)

        # Deserialize back to dict
        data_dict = json.loads(json_bytes.decode('utf-8'))

        # Validate all fields are present
        assert data_dict['temperature'] == 23.7
        assert data_dict['sensor_id'] == "sensor_42"
        assert data_dict['location'] == "server_room"
        assert data_dict['humidity'] == 45.2
        assert 'id' in data_dict
        assert 'timestamp' in data_dict

    def test_sensor_reading_deserialization(self):
        """Test JSON deserialization from bytes."""
        json_data = {
            'id': 'test-uuid-123',
            'timestamp': '2024-04-10T10:00:00+00:00',
            'temperature': 22.5,
            'sensor_id': 'sensor_test',
            'location': 'office',
            'humidity': 50.0
        }
        json_bytes = json.dumps(json_data).encode('utf-8')

        reading = ConsumerSensorReading.from_json_bytes(json_bytes)

        assert reading.id == 'test-uuid-123'
        assert reading.temperature == 22.5
        assert reading.sensor_id == 'sensor_test'
        assert reading.location == 'office'
        assert reading.humidity == 50.0


class TestWindowedAggregator:
    """Test the WindowedAggregator class."""

    def test_windowed_aggregator_creation(self):
        """Test creating a windowed aggregator."""
        aggregator = WindowedAggregator(window_size_seconds=60)
        assert aggregator.window_size == 60
        assert len(aggregator.sensor_data) == 0

    def test_add_reading_and_basic_aggregation(self):
        """Test adding readings and calculating basic aggregations."""
        aggregator = WindowedAggregator(window_size_seconds=60)

        # Create test readings
        reading1 = SensorReading(temperature=20.0, sensor_id="sensor_1")
        reading2 = SensorReading(temperature=25.0, sensor_id="sensor_1")
        reading3 = SensorReading(temperature=30.0, sensor_id="sensor_1")

        # Add readings
        aggregator.add_reading(reading1)
        aggregator.add_reading(reading2)
        aggregator.add_reading(reading3)

        # Calculate aggregations
        results = aggregator.calculate_aggregations()

        assert "sensor_1" in results
        stats = results["sensor_1"]
        assert stats['count'] == 3
        assert stats['mean'] == 25.0  # (20 + 25 + 30) / 3
        assert stats['min'] == 20.0
        assert stats['max'] == 30.0
        assert stats['std'] == 5.0  # Standard deviation

    def test_multiple_sensors_aggregation(self):
        """Test aggregation with multiple sensors."""
        aggregator = WindowedAggregator(window_size_seconds=60)

        # Add readings for different sensors
        aggregator.add_reading(SensorReading(temperature=20.0, sensor_id="sensor_1"))
        aggregator.add_reading(SensorReading(temperature=30.0, sensor_id="sensor_2"))
        aggregator.add_reading(SensorReading(temperature=25.0, sensor_id="sensor_1"))

        results = aggregator.calculate_aggregations()

        assert "sensor_1" in results
        assert "sensor_2" in results
        assert results["sensor_1"]["count"] == 2
        assert results["sensor_2"]["count"] == 1
        assert results["sensor_1"]["mean"] == 22.5  # (20 + 25) / 2
        assert results["sensor_2"]["mean"] == 30.0

    @patch('time.time')
    def test_window_cleanup(self, mock_time):
        """Test that old data is cleaned up from the window."""
        # Mock current time
        base_time = time.time()
        mock_time.return_value = base_time

        aggregator = WindowedAggregator(window_size_seconds=60)

        # Create reading with timestamp in the past
        old_reading = SensorReading(temperature=20.0, sensor_id="sensor_1")
        # Manually set old timestamp
        old_timestamp = datetime.fromtimestamp(base_time - 120, tz=timezone.utc).isoformat()
        old_reading.timestamp = old_timestamp

        aggregator.add_reading(old_reading)

        # Move time forward to trigger cleanup
        mock_time.return_value = base_time + 61

        # Add new reading (this should trigger cleanup)
        new_reading = SensorReading(temperature=25.0, sensor_id="sensor_1")
        aggregator.add_reading(new_reading)

        results = aggregator.calculate_aggregations()

        # Should only have the new reading, old one should be cleaned up
        assert results["sensor_1"]["count"] == 1
        assert results["sensor_1"]["mean"] == 25.0


class TestTemperatureProducer:
    """Test the TemperatureProducer class with mocked dependencies."""

    def test_producer_initialization(self):
        """Test producer initialization with default parameters."""
        producer = TemperatureProducer()

        assert producer.bootstrap_servers == "localhost:9092"
        assert producer.topic == "learning-data"
        assert producer.num_sensors == 10
        assert producer.producer is None
        assert producer.admin_client is None

    @patch('producer.KafkaAdminClient')
    def test_ensure_topic_exists_creation(self, mock_admin_client):
        """Test topic creation when topic doesn't exist."""
        # Setup mocks
        mock_admin = Mock()
        mock_admin_client.return_value = mock_admin

        producer = TemperatureProducer()
        producer.ensure_topic_exists()

        # Verify admin client was created and topic creation was attempted
        mock_admin_client.assert_called_once()
        mock_admin.create_topics.assert_called_once()

        # Verify topic configuration
        call_args = mock_admin.create_topics.call_args[0][0]  # First positional arg
        new_topic = call_args[0]  # First topic in list

        assert new_topic.name == "learning-data"
        assert new_topic.num_partitions == 6
        assert new_topic.replication_factor == 2

    @patch('producer.KafkaAdminClient')
    def test_ensure_topic_exists_already_exists(self, mock_admin_client):
        """Test handling when topic already exists."""
        from kafka.errors import TopicAlreadyExistsError

        # Setup mocks
        mock_admin = Mock()
        mock_admin.create_topics.side_effect = TopicAlreadyExistsError()
        mock_admin_client.return_value = mock_admin

        producer = TemperatureProducer()

        # Should not raise exception when topic already exists
        producer.ensure_topic_exists()

        mock_admin.create_topics.assert_called_once()

    def test_generate_sensor_reading(self):
        """Test sensor reading generation."""
        producer = TemperatureProducer(num_sensors=5)
        reading = producer.generate_sensor_reading()

        assert isinstance(reading, SensorReading)
        assert 15.0 <= reading.temperature <= 35.0
        assert reading.sensor_id.startswith("sensor_")
        assert reading.location is not None
        assert reading.humidity is not None

    @patch('producer.KafkaProducer')
    @patch('producer.KafkaAdminClient')
    def test_delivery_callback_success(self, mock_admin_client, mock_producer_class):
        """Test successful message delivery callback."""
        # Setup mocks
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        mock_admin = Mock()
        mock_admin_client.return_value = mock_admin

        producer = TemperatureProducer()
        producer.ensure_topic_exists()
        producer.producer = producer._create_producer()

        # Mock record metadata
        mock_metadata = Mock()
        mock_metadata.topic = "test-topic"
        mock_metadata.partition = 1
        mock_metadata.offset = 100

        # Test successful delivery
        initial_count = producer.messages_sent
        producer._delivery_callback(mock_metadata, None)

        assert producer.messages_sent == initial_count + 1

    def test_delivery_callback_failure(self):
        """Test failed message delivery callback."""
        producer = TemperatureProducer()

        # Mock exception
        mock_exception = Exception("Delivery failed")

        # Test failed delivery (should not increment counter)
        initial_count = producer.messages_sent
        producer._delivery_callback(None, mock_exception)

        assert producer.messages_sent == initial_count


class TestTemperatureConsumer:
    """Test the TemperatureConsumer class with mocked dependencies."""

    def test_consumer_initialization(self):
        """Test consumer initialization with default parameters."""
        consumer = TemperatureConsumer()

        assert consumer.bootstrap_servers == "localhost:9092"
        assert consumer.topic == "learning-data"
        assert consumer.consumer_group == "temperature-aggregator"
        assert consumer.consumer is None
        assert isinstance(consumer.aggregator, WindowedAggregator)
        assert consumer.running is True

    @patch('consumer.KafkaConsumer')
    def test_consumer_creation(self, mock_consumer_class):
        """Test Kafka consumer creation with proper configuration."""
        consumer = TemperatureConsumer()
        consumer._create_consumer()

        # Verify consumer was created with correct parameters
        mock_consumer_class.assert_called_once()
        call_args = mock_consumer_class.call_args

        # Check positional args (topic)
        assert call_args[0][0] == "learning-data"

        # Check keyword args
        kwargs = call_args[1]
        assert kwargs['bootstrap_servers'] == "localhost:9092"
        assert kwargs['group_id'] == "temperature-aggregator"
        assert kwargs['auto_offset_reset'] == 'earliest'
        assert kwargs['enable_auto_commit'] is False

    @patch('consumer.KafkaConsumer')
    def test_signal_handler(self, mock_consumer_class):
        """Test graceful shutdown signal handling."""
        consumer = TemperatureConsumer()

        # Verify consumer is initially running
        assert consumer.running is True

        # Simulate signal
        consumer._signal_handler(2, None)  # SIGINT

        # Verify consumer is marked for shutdown
        assert consumer.running is False


# Integration-style tests with more complex scenarios
class TestIntegrationScenarios:
    """Integration tests that combine multiple components."""

    def test_producer_consumer_data_flow_simulation(self):
        """Simulate data flow from producer to consumer without actual Kafka."""
        # Create producer and generate some readings
        producer = TemperatureProducer(num_sensors=3)
        readings = [producer.generate_sensor_reading() for _ in range(10)]

        # Create consumer aggregator and process the readings
        aggregator = WindowedAggregator(window_size_seconds=60)
        for reading in readings:
            aggregator.add_reading(reading)

        # Verify aggregations
        results = aggregator.calculate_aggregations()
        assert len(results) <= 3  # At most 3 sensors (could be fewer due to randomness)

        for sensor_id, stats in results.items():
            assert stats['count'] > 0
            assert 15.0 <= stats['mean'] <= 35.0
            assert 15.0 <= stats['min'] <= 35.0
            assert 15.0 <= stats['max'] <= 35.0

    def test_serialization_deserialization_compatibility(self):
        """Test that producer and consumer use compatible serialization."""
        # Create reading with producer model
        producer_reading = SensorReading(
            temperature=23.5,
            sensor_id="sensor_test",
            location="test_location",
            humidity=65.0
        )

        # Serialize with producer
        json_bytes = producer_reading.to_json_bytes()

        # Deserialize with consumer
        consumer_reading = ConsumerSensorReading.from_json_bytes(json_bytes)

        # Verify compatibility
        assert consumer_reading.temperature == producer_reading.temperature
        assert consumer_reading.sensor_id == producer_reading.sensor_id
        assert consumer_reading.location == producer_reading.location
        assert consumer_reading.humidity == producer_reading.humidity


# Fixtures for common test data
@pytest.fixture
def sample_sensor_readings():
    """Provide sample sensor readings for tests."""
    return [
        SensorReading(temperature=20.0, sensor_id="sensor_1", location="office"),
        SensorReading(temperature=25.0, sensor_id="sensor_1", location="office"),
        SensorReading(temperature=30.0, sensor_id="sensor_2", location="warehouse"),
        SensorReading(temperature=22.0, sensor_id="sensor_2", location="warehouse"),
    ]


@pytest.fixture
def mock_kafka_message():
    """Create a mock Kafka message for testing."""
    message = Mock()
    message.topic = "test-topic"
    message.partition = 1
    message.offset = 100
    message.key = b"sensor_1"

    # Create realistic message value
    reading_data = {
        'id': 'test-123',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'temperature': 25.5,
        'sensor_id': 'sensor_1',
        'location': 'test_location'
    }
    message.value = json.dumps(reading_data).encode('utf-8')

    return message


if __name__ == '__main__':
    # Allow running tests directly
    pytest.main([__file__, '-v'])