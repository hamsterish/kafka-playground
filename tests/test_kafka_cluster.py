"""
End-to-End Integration Tests for Kafka Playground

Educational Focus:
- Testing complete Kafka cluster deployment and functionality
- Validating monitoring stack integration
- Verifying data flow from producer to consumer
- Testing cluster resilience and recovery patterns

These tests demonstrate how to validate distributed systems
and ensure all components work together correctly.
"""

import json
import pytest
import time
import requests
import subprocess
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import uuid
import statistics

# Import test utilities
from testcontainers import DockerContainer
from testcontainers.compose import DockerCompose


class KafkaClusterTester:
    """
    Comprehensive integration testing for Kafka playground environment.

    This class provides methods to test:
    - Cluster deployment and health
    - Data producer and consumer functionality
    - Monitoring stack integration
    - Chaos testing scenarios
    - Performance characteristics
    """

    def __init__(self, namespace: str = "kafka-learning"):
        self.namespace = namespace
        self.kubectl_timeout = 60

    def run_kubectl_command(self, cmd: str, timeout: int = None) -> str:
        """Execute kubectl command and return output."""
        timeout = timeout or self.kubectl_timeout
        try:
            result = subprocess.run(
                f"kubectl {cmd}",
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            if result.returncode != 0:
                raise RuntimeError(f"kubectl command failed: {result.stderr}")
            return result.stdout.strip()
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"kubectl command timed out after {timeout}s")

    def check_namespace_exists(self) -> bool:
        """Verify the target namespace exists."""
        try:
            self.run_kubectl_command(f"get namespace {self.namespace}")
            return True
        except RuntimeError:
            return False

    def get_pod_status(self) -> Dict[str, Any]:
        """Get comprehensive pod status information."""
        try:
            # Get all pods in namespace
            output = self.run_kubectl_command(
                f"get pods -n {self.namespace} -o json"
            )
            pods_data = json.loads(output)

            status_summary = {
                "total_pods": 0,
                "running_pods": 0,
                "pending_pods": 0,
                "failed_pods": 0,
                "kafka_pods": [],
                "zookeeper_pods": [],
                "other_pods": []
            }

            for pod in pods_data.get("items", []):
                pod_name = pod["metadata"]["name"]
                pod_status = pod["status"]["phase"]
                labels = pod["metadata"].get("labels", {})

                status_summary["total_pods"] += 1

                if pod_status == "Running":
                    status_summary["running_pods"] += 1
                elif pod_status == "Pending":
                    status_summary["pending_pods"] += 1
                elif pod_status == "Failed":
                    status_summary["failed_pods"] += 1

                # Categorize pods by type
                if labels.get("app") == "kafka":
                    status_summary["kafka_pods"].append({
                        "name": pod_name,
                        "status": pod_status,
                        "ready": self._is_pod_ready(pod)
                    })
                elif labels.get("app") == "zookeeper":
                    status_summary["zookeeper_pods"].append({
                        "name": pod_name,
                        "status": pod_status,
                        "ready": self._is_pod_ready(pod)
                    })
                else:
                    status_summary["other_pods"].append({
                        "name": pod_name,
                        "status": pod_status,
                        "ready": self._is_pod_ready(pod)
                    })

            return status_summary

        except Exception as e:
            raise RuntimeError(f"Failed to get pod status: {e}")

    def _is_pod_ready(self, pod: Dict) -> bool:
        """Check if a pod is ready based on its conditions."""
        conditions = pod.get("status", {}).get("conditions", [])
        for condition in conditions:
            if condition["type"] == "Ready":
                return condition["status"] == "True"
        return False

    def test_kafka_broker_connectivity(self) -> Dict[str, Any]:
        """Test connectivity to all Kafka brokers."""
        pod_status = self.get_pod_status()
        kafka_pods = pod_status["kafka_pods"]

        connectivity_results = {
            "total_brokers": len(kafka_pods),
            "responsive_brokers": 0,
            "broker_details": []
        }

        for kafka_pod in kafka_pods:
            pod_name = kafka_pod["name"]
            try:
                # Test broker API connectivity
                self.run_kubectl_command(
                    f"exec {pod_name} -n {self.namespace} -- "
                    f"kafka-broker-api-versions.sh --bootstrap-server localhost:9092",
                    timeout=30
                )
                connectivity_results["responsive_brokers"] += 1
                connectivity_results["broker_details"].append({
                    "name": pod_name,
                    "responsive": True,
                    "error": None
                })
            except Exception as e:
                connectivity_results["broker_details"].append({
                    "name": pod_name,
                    "responsive": False,
                    "error": str(e)
                })

        return connectivity_results

    def test_topic_operations(self) -> Dict[str, Any]:
        """Test topic creation, listing, and deletion."""
        kafka_pods = self.get_pod_status()["kafka_pods"]
        if not kafka_pods:
            raise RuntimeError("No Kafka pods found for topic testing")

        kafka_pod = kafka_pods[0]["name"]
        test_topic = f"integration-test-{uuid.uuid4().hex[:8]}"

        test_results = {
            "topic_name": test_topic,
            "create_success": False,
            "list_success": False,
            "delete_success": False,
            "error": None
        }

        try:
            # Create test topic
            self.run_kubectl_command(
                f"exec {kafka_pod} -n {self.namespace} -- "
                f"kafka-topics.sh --bootstrap-server localhost:9092 "
                f"--create --topic {test_topic} --partitions 3 --replication-factor 2",
                timeout=30
            )
            test_results["create_success"] = True

            # List topics and verify our topic exists
            topics_output = self.run_kubectl_command(
                f"exec {kafka_pod} -n {self.namespace} -- "
                f"kafka-topics.sh --bootstrap-server localhost:9092 --list",
                timeout=30
            )
            if test_topic in topics_output:
                test_results["list_success"] = True

            # Clean up - delete test topic
            self.run_kubectl_command(
                f"exec {kafka_pod} -n {self.namespace} -- "
                f"kafka-topics.sh --bootstrap-server localhost:9092 "
                f"--delete --topic {test_topic}",
                timeout=30
            )
            test_results["delete_success"] = True

        except Exception as e:
            test_results["error"] = str(e)

        return test_results

    def test_message_flow(self) -> Dict[str, Any]:
        """Test end-to-end message production and consumption."""
        kafka_pods = self.get_pod_status()["kafka_pods"]
        if not kafka_pods:
            raise RuntimeError("No Kafka pods found for message flow testing")

        kafka_pod = kafka_pods[0]["name"]
        test_topic = f"message-flow-test-{uuid.uuid4().hex[:8]}"
        test_messages = [
            f"Test message {i} at {datetime.now().isoformat()}"
            for i in range(5)
        ]

        test_results = {
            "topic_name": test_topic,
            "messages_sent": 0,
            "messages_received": 0,
            "message_match": False,
            "latency_ms": None,
            "error": None
        }

        try:
            # Create test topic
            self.run_kubectl_command(
                f"exec {kafka_pod} -n {self.namespace} -- "
                f"kafka-topics.sh --bootstrap-server localhost:9092 "
                f"--create --topic {test_topic} --partitions 1 --replication-factor 1",
                timeout=30
            )

            start_time = time.time()

            # Send test messages
            for message in test_messages:
                self.run_kubectl_command(
                    f"exec {kafka_pod} -n {self.namespace} -- /bin/bash -c "
                    f"\"echo '{message}' | kafka-console-producer.sh "
                    f"--bootstrap-server localhost:9092 --topic {test_topic}\"",
                    timeout=30
                )
                test_results["messages_sent"] += 1

            # Consume messages
            consumed_output = self.run_kubectl_command(
                f"exec {kafka_pod} -n {self.namespace} -- "
                f"timeout 10 kafka-console-consumer.sh "
                f"--bootstrap-server localhost:9092 --topic {test_topic} "
                f"--from-beginning --max-messages {len(test_messages)}",
                timeout=15
            )

            end_time = time.time()
            test_results["latency_ms"] = int((end_time - start_time) * 1000)

            # Verify consumed messages
            consumed_lines = [line.strip() for line in consumed_output.split('\n') if line.strip()]
            test_results["messages_received"] = len(consumed_lines)

            # Check if all sent messages were received
            test_results["message_match"] = (
                test_results["messages_sent"] == test_results["messages_received"]
                and all(msg in consumed_lines for msg in test_messages)
            )

            # Clean up
            self.run_kubectl_command(
                f"exec {kafka_pod} -n {self.namespace} -- "
                f"kafka-topics.sh --bootstrap-server localhost:9092 "
                f"--delete --topic {test_topic}",
                timeout=30
            )

        except Exception as e:
            test_results["error"] = str(e)

        return test_results

    def test_zookeeper_ensemble(self) -> Dict[str, Any]:
        """Test ZooKeeper ensemble health and connectivity."""
        pod_status = self.get_pod_status()
        zk_pods = pod_status["zookeeper_pods"]

        ensemble_results = {
            "total_nodes": len(zk_pods),
            "healthy_nodes": 0,
            "leader_count": 0,
            "follower_count": 0,
            "node_details": []
        }

        for zk_pod in zk_pods:
            pod_name = zk_pod["name"]
            try:
                # Test ZooKeeper health
                health_output = self.run_kubectl_command(
                    f"exec {zk_pod['name']} -n {self.namespace} -- /bin/bash -c "
                    f"\"echo ruok | nc localhost 2181\"",
                    timeout=10
                )

                is_healthy = "imok" in health_output

                # Get ZooKeeper mode (leader/follower)
                try:
                    mode_output = self.run_kubectl_command(
                        f"exec {zk_pod['name']} -n {self.namespace} -- /bin/bash -c "
                        f"\"echo srvr | nc localhost 2181 | grep Mode\"",
                        timeout=10
                    )
                    mode = "leader" if "leader" in mode_output.lower() else "follower"
                except:
                    mode = "unknown"

                if is_healthy:
                    ensemble_results["healthy_nodes"] += 1
                    if mode == "leader":
                        ensemble_results["leader_count"] += 1
                    elif mode == "follower":
                        ensemble_results["follower_count"] += 1

                ensemble_results["node_details"].append({
                    "name": pod_name,
                    "healthy": is_healthy,
                    "mode": mode
                })

            except Exception as e:
                ensemble_results["node_details"].append({
                    "name": pod_name,
                    "healthy": False,
                    "mode": "error",
                    "error": str(e)
                })

        return ensemble_results

    def test_monitoring_stack(self) -> Dict[str, Any]:
        """Test monitoring stack components (VictoriaMetrics, Grafana, vmagent)."""
        monitoring_results = {
            "victoriametrics": {"status": "unknown", "accessible": False},
            "grafana": {"status": "unknown", "accessible": False},
            "vmagent": {"status": "unknown", "accessible": False},
            "kafka_exporter": {"status": "unknown", "accessible": False}
        }

        try:
            # Check VictoriaMetrics
            vm_status = self.run_kubectl_command(
                "get deployment victoriametrics -n kafka-monitoring -o jsonpath='{.status.readyReplicas}'"
            )
            monitoring_results["victoriametrics"]["status"] = "ready" if vm_status == "1" else "not_ready"

            # Check Grafana
            grafana_status = self.run_kubectl_command(
                "get deployment grafana -n kafka-monitoring -o jsonpath='{.status.readyReplicas}'"
            )
            monitoring_results["grafana"]["status"] = "ready" if grafana_status == "1" else "not_ready"

            # Check vmagent
            vmagent_status = self.run_kubectl_command(
                "get deployment vmagent -n kafka-monitoring -o jsonpath='{.status.readyReplicas}'"
            )
            monitoring_results["vmagent"]["status"] = "ready" if vmagent_status == "1" else "not_ready"

            # Check kafka-exporter
            try:
                exporter_status = self.run_kubectl_command(
                    f"get deployment kafka-exporter -n {self.namespace} -o jsonpath='{{.status.readyReplicas}}'"
                )
                monitoring_results["kafka_exporter"]["status"] = "ready" if exporter_status == "1" else "not_ready"
            except:
                monitoring_results["kafka_exporter"]["status"] = "not_found"

        except Exception as e:
            monitoring_results["error"] = str(e)

        return monitoring_results

    def test_cluster_performance(self) -> Dict[str, Any]:
        """Test basic cluster performance characteristics."""
        kafka_pods = self.get_pod_status()["kafka_pods"]
        if not kafka_pods:
            raise RuntimeError("No Kafka pods found for performance testing")

        kafka_pod = kafka_pods[0]["name"]
        test_topic = f"perf-test-{uuid.uuid4().hex[:8]}"
        message_count = 100
        message_size = 1024  # 1KB messages

        perf_results = {
            "message_count": message_count,
            "message_size_bytes": message_size,
            "producer_throughput_msgs_sec": 0,
            "consumer_throughput_msgs_sec": 0,
            "end_to_end_latency_ms": 0,
            "error": None
        }

        try:
            # Create performance test topic
            self.run_kubectl_command(
                f"exec {kafka_pod} -n {self.namespace} -- "
                f"kafka-topics.sh --bootstrap-server localhost:9092 "
                f"--create --topic {test_topic} --partitions 3 --replication-factor 2",
                timeout=30
            )

            # Generate test message
            test_message = "x" * message_size

            # Producer performance test
            producer_start = time.time()
            for i in range(message_count):
                self.run_kubectl_command(
                    f"exec {kafka_pod} -n {self.namespace} -- /bin/bash -c "
                    f"\"echo 'msg-{i}: {test_message}' | kafka-console-producer.sh "
                    f"--bootstrap-server localhost:9092 --topic {test_topic}\"",
                    timeout=5
                )
            producer_end = time.time()

            producer_duration = producer_end - producer_start
            perf_results["producer_throughput_msgs_sec"] = round(message_count / producer_duration, 2)

            # Consumer performance test
            consumer_start = time.time()
            self.run_kubectl_command(
                f"exec {kafka_pod} -n {self.namespace} -- "
                f"timeout 30 kafka-console-consumer.sh "
                f"--bootstrap-server localhost:9092 --topic {test_topic} "
                f"--from-beginning --max-messages {message_count}",
                timeout=35
            )
            consumer_end = time.time()

            consumer_duration = consumer_end - consumer_start
            perf_results["consumer_throughput_msgs_sec"] = round(message_count / consumer_duration, 2)
            perf_results["end_to_end_latency_ms"] = round(
                (producer_duration + consumer_duration) * 1000 / message_count, 2
            )

            # Clean up
            self.run_kubectl_command(
                f"exec {kafka_pod} -n {self.namespace} -- "
                f"kafka-topics.sh --bootstrap-server localhost:9092 "
                f"--delete --topic {test_topic}",
                timeout=30
            )

        except Exception as e:
            perf_results["error"] = str(e)

        return perf_results


# Test Classes using pytest

class TestKafkaClusterDeployment:
    """Test suite for Kafka cluster deployment validation."""

    @pytest.fixture(scope="class")
    def cluster_tester(self):
        """Provide a cluster tester instance."""
        return KafkaClusterTester()

    def test_namespace_exists(self, cluster_tester):
        """Test that the Kafka namespace exists."""
        assert cluster_tester.check_namespace_exists(), "Kafka namespace not found"

    def test_pod_status(self, cluster_tester):
        """Test that all pods are running properly."""
        pod_status = cluster_tester.get_pod_status()

        # At least some pods should exist
        assert pod_status["total_pods"] > 0, "No pods found in namespace"

        # All pods should be running
        assert pod_status["failed_pods"] == 0, f"Failed pods detected: {pod_status['failed_pods']}"

        # Kafka and ZooKeeper pods should exist
        assert len(pod_status["kafka_pods"]) >= 3, "Expected at least 3 Kafka brokers"
        assert len(pod_status["zookeeper_pods"]) >= 3, "Expected at least 3 ZooKeeper nodes"

        # All Kafka and ZooKeeper pods should be ready
        for kafka_pod in pod_status["kafka_pods"]:
            assert kafka_pod["ready"], f"Kafka pod {kafka_pod['name']} is not ready"

        for zk_pod in pod_status["zookeeper_pods"]:
            assert zk_pod["ready"], f"ZooKeeper pod {zk_pod['name']} is not ready"

    def test_kafka_broker_connectivity(self, cluster_tester):
        """Test connectivity to Kafka brokers."""
        connectivity = cluster_tester.test_kafka_broker_connectivity()

        assert connectivity["total_brokers"] >= 3, "Expected at least 3 Kafka brokers"
        assert connectivity["responsive_brokers"] == connectivity["total_brokers"], \
            f"Not all brokers responsive: {connectivity['responsive_brokers']}/{connectivity['total_brokers']}"

    def test_zookeeper_ensemble_health(self, cluster_tester):
        """Test ZooKeeper ensemble health."""
        ensemble = cluster_tester.test_zookeeper_ensemble()

        assert ensemble["total_nodes"] >= 3, "Expected at least 3 ZooKeeper nodes"
        assert ensemble["healthy_nodes"] == ensemble["total_nodes"], \
            f"Unhealthy ZooKeeper nodes: {ensemble['total_nodes'] - ensemble['healthy_nodes']}"

        # Should have exactly one leader
        assert ensemble["leader_count"] == 1, f"Expected 1 leader, found {ensemble['leader_count']}"
        assert ensemble["follower_count"] >= 2, f"Expected at least 2 followers, found {ensemble['follower_count']}"


class TestKafkaFunctionality:
    """Test suite for Kafka functionality validation."""

    @pytest.fixture(scope="class")
    def cluster_tester(self):
        """Provide a cluster tester instance."""
        return KafkaClusterTester()

    def test_topic_operations(self, cluster_tester):
        """Test basic topic operations."""
        topic_test = cluster_tester.test_topic_operations()

        assert topic_test["create_success"], f"Topic creation failed: {topic_test.get('error')}"
        assert topic_test["list_success"], "Topic listing failed"
        assert topic_test["delete_success"], "Topic deletion failed"

    def test_message_flow(self, cluster_tester):
        """Test end-to-end message flow."""
        message_test = cluster_tester.test_message_flow()

        assert message_test["messages_sent"] > 0, "No messages were sent"
        assert message_test["messages_received"] == message_test["messages_sent"], \
            f"Message count mismatch: sent {message_test['messages_sent']}, received {message_test['messages_received']}"
        assert message_test["message_match"], "Message content verification failed"
        assert message_test["latency_ms"] is not None and message_test["latency_ms"] < 10000, \
            f"Message latency too high: {message_test['latency_ms']}ms"

    def test_cluster_performance(self, cluster_tester):
        """Test basic cluster performance."""
        perf_test = cluster_tester.test_cluster_performance()

        assert perf_test["producer_throughput_msgs_sec"] > 0, \
            f"Producer throughput test failed: {perf_test.get('error')}"
        assert perf_test["consumer_throughput_msgs_sec"] > 0, \
            f"Consumer throughput test failed: {perf_test.get('error')}"

        # Performance should be reasonable for educational environment
        assert perf_test["producer_throughput_msgs_sec"] > 10, \
            f"Producer throughput too low: {perf_test['producer_throughput_msgs_sec']} msg/sec"
        assert perf_test["end_to_end_latency_ms"] < 1000, \
            f"End-to-end latency too high: {perf_test['end_to_end_latency_ms']}ms"


class TestMonitoringIntegration:
    """Test suite for monitoring stack integration."""

    @pytest.fixture(scope="class")
    def cluster_tester(self):
        """Provide a cluster tester instance."""
        return KafkaClusterTester()

    def test_monitoring_stack_deployment(self, cluster_tester):
        """Test monitoring components are deployed correctly."""
        monitoring = cluster_tester.test_monitoring_stack()

        # Core monitoring components should be ready
        assert monitoring["victoriametrics"]["status"] == "ready", \
            "VictoriaMetrics is not ready"
        assert monitoring["grafana"]["status"] == "ready", \
            "Grafana is not ready"
        assert monitoring["vmagent"]["status"] == "ready", \
            "vmagent is not ready"

    @pytest.mark.skip(reason="Requires port-forward for HTTP testing")
    def test_grafana_accessibility(self, cluster_tester):
        """Test Grafana dashboard accessibility (requires port-forward)."""
        # This test would require setting up port-forwarding
        # kubectl port-forward -n kafka-monitoring svc/grafana 3000:3000
        try:
            response = requests.get("http://localhost:3000/api/health", timeout=5)
            assert response.status_code == 200, "Grafana health check failed"
        except requests.exceptions.RequestException:
            pytest.skip("Grafana not accessible via localhost:3000")

    @pytest.mark.skip(reason="Requires port-forward for metrics testing")
    def test_victoriametrics_metrics(self, cluster_tester):
        """Test VictoriaMetrics metrics endpoint."""
        # This test would require setting up port-forwarding
        # kubectl port-forward -n kafka-monitoring svc/victoriametrics 8428:8428
        try:
            response = requests.get("http://localhost:8428/api/v1/query?query=up", timeout=5)
            assert response.status_code == 200, "VictoriaMetrics query failed"
            data = response.json()
            assert data.get("status") == "success", "VictoriaMetrics query unsuccessful"
        except requests.exceptions.RequestException:
            pytest.skip("VictoriaMetrics not accessible via localhost:8428")


# Integration test runner
def run_comprehensive_tests():
    """
    Run comprehensive integration tests and generate a report.

    This function can be called directly to run all tests and
    generate a detailed report of the Kafka playground health.
    """
    print("🧪 Starting Comprehensive Kafka Playground Tests")
    print("=" * 60)

    tester = KafkaClusterTester()

    # Test results storage
    results = {
        "timestamp": datetime.now().isoformat(),
        "namespace": tester.namespace,
        "tests": {}
    }

    # Run individual test categories
    test_categories = [
        ("Cluster Deployment", [
            ("Namespace Check", lambda: tester.check_namespace_exists()),
            ("Pod Status", lambda: tester.get_pod_status()),
            ("Broker Connectivity", lambda: tester.test_kafka_broker_connectivity()),
            ("ZooKeeper Ensemble", lambda: tester.test_zookeeper_ensemble()),
        ]),
        ("Kafka Functionality", [
            ("Topic Operations", lambda: tester.test_topic_operations()),
            ("Message Flow", lambda: tester.test_message_flow()),
            ("Performance Test", lambda: tester.test_cluster_performance()),
        ]),
        ("Monitoring Stack", [
            ("Component Status", lambda: tester.test_monitoring_stack()),
        ])
    ]

    overall_success = True

    for category_name, tests in test_categories:
        print(f"\n📋 {category_name} Tests")
        print("-" * 40)

        category_results = {}
        results["tests"][category_name] = category_results

        for test_name, test_func in tests:
            try:
                print(f"  🔍 Running: {test_name}...")
                test_result = test_func()

                # Determine if test passed based on result structure
                test_passed = True
                if isinstance(test_result, dict):
                    if "error" in test_result and test_result["error"]:
                        test_passed = False
                    elif test_name == "Pod Status":
                        test_passed = test_result.get("failed_pods", 0) == 0
                    elif test_name == "Broker Connectivity":
                        test_passed = (test_result.get("responsive_brokers", 0) ==
                                     test_result.get("total_brokers", 0))
                    elif test_name == "ZooKeeper Ensemble":
                        test_passed = (test_result.get("healthy_nodes", 0) ==
                                     test_result.get("total_nodes", 0) and
                                     test_result.get("leader_count", 0) == 1)

                status = "✅ PASS" if test_passed else "❌ FAIL"
                print(f"    {status}: {test_name}")

                if not test_passed:
                    overall_success = False

                category_results[test_name] = {
                    "status": "pass" if test_passed else "fail",
                    "result": test_result
                }

            except Exception as e:
                print(f"    ❌ FAIL: {test_name} - {str(e)}")
                overall_success = False
                category_results[test_name] = {
                    "status": "error",
                    "error": str(e)
                }

    # Final summary
    print("\n" + "=" * 60)
    if overall_success:
        print("🎉 ALL TESTS PASSED - Kafka Playground is fully operational!")
    else:
        print("⚠️  SOME TESTS FAILED - Check the details above")

    print(f"\n📊 Test Summary:")
    print(f"   Timestamp: {results['timestamp']}")
    print(f"   Namespace: {results['namespace']}")

    # Save results to file
    import json
    results_file = f"integration-test-results-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"   Results saved to: {results_file}")
    print("\n🔍 Next steps:")
    print("   - Review any failed tests")
    print("   - Check pod logs: kubectl logs <pod-name> -n kafka-learning")
    print("   - Monitor cluster health: ./chaos/monitor-cluster-health.sh --once")
    print("   - Access Grafana: kubectl port-forward -n kafka-monitoring svc/grafana 3000:3000")

    return overall_success


if __name__ == "__main__":
    # Allow running tests directly
    success = run_comprehensive_tests()
    exit(0 if success else 1)