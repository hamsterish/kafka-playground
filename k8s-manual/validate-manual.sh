#!/bin/bash
# Kafka Cluster Validation Script
# Educational tool for validating manual Kubernetes deployment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_command() {
    if command -v $1 &> /dev/null; then
        log_info "✓ $1 is available"
    else
        log_error "✗ $1 is not available. Please install it."
        exit 1
    fi
}

wait_for_pod() {
    local namespace=$1
    local label_selector=$2
    local timeout=${3:-300}

    log_info "Waiting for pods with selector '$label_selector' in namespace '$namespace'..."

    if kubectl wait --for=condition=ready pod \
        -l "$label_selector" \
        -n "$namespace" \
        --timeout="${timeout}s" &>/dev/null; then
        log_info "✓ Pods are ready"
        return 0
    else
        log_error "✗ Pods failed to become ready within ${timeout} seconds"
        return 1
    fi
}

# Main validation
echo "==================================="
echo "Kafka Playground Validation Script"
echo "==================================="

# Check prerequisites
log_info "Checking prerequisites..."
check_command kubectl
check_command nc

# Check if namespaces exist
log_info "Checking namespaces..."
if kubectl get namespace kafka-learning &>/dev/null; then
    log_info "✓ kafka-learning namespace exists"
else
    log_error "✗ kafka-learning namespace not found"
    exit 1
fi

if kubectl get namespace kafka-monitoring &>/dev/null; then
    log_info "✓ kafka-monitoring namespace exists"
else
    log_error "✗ kafka-monitoring namespace not found"
    exit 1
fi

# Check ZooKeeper cluster
log_info "Validating ZooKeeper cluster..."
zk_replicas=$(kubectl get statefulset zk -n kafka-learning -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [ "$zk_replicas" = "3" ]; then
    log_info "✓ ZooKeeper ensemble is healthy (3/3 replicas ready)"
else
    log_warn "⚠ ZooKeeper cluster status: $zk_replicas/3 replicas ready"
fi

# Test ZooKeeper connectivity
log_info "Testing ZooKeeper connectivity..."
for i in {0..2}; do
    if kubectl exec zk-$i -n kafka-learning -- echo ruok | kubectl exec zk-$i -n kafka-learning -- nc localhost 2181 | grep -q imok 2>/dev/null; then
        log_info "✓ zk-$i is responding to health checks"
    else
        log_warn "⚠ zk-$i health check failed"
    fi
done

# Check Kafka brokers
log_info "Validating Kafka brokers..."
kafka_replicas=$(kubectl get statefulset kafka -n kafka-learning -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [ "$kafka_replicas" = "3" ]; then
    log_info "✓ Kafka cluster is healthy (3/3 brokers ready)"
else
    log_warn "⚠ Kafka cluster status: $kafka_replicas/3 brokers ready"
fi

# Test Kafka broker connectivity
log_info "Testing Kafka broker connectivity..."
if kubectl exec kafka-0 -n kafka-learning -- kafka-broker-api-versions.sh --bootstrap-server localhost:9092 &>/dev/null; then
    log_info "✓ Kafka brokers are accepting connections"
else
    log_warn "⚠ Kafka broker connectivity test failed"
fi

# List topics (should show internal topics)
log_info "Listing Kafka topics..."
topics=$(kubectl exec kafka-0 -n kafka-learning -- kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null || echo "")
if [ -n "$topics" ]; then
    log_info "✓ Kafka topics:"
    echo "$topics" | sed 's/^/    /'
else
    log_warn "⚠ No topics found or topic listing failed"
fi

# Test topic creation and message production
log_info "Testing topic creation and message flow..."
TEST_TOPIC="validation-test-$(date +%s)"

# Create test topic
if kubectl exec kafka-0 -n kafka-learning -- kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic "$TEST_TOPIC" \
    --partitions 3 \
    --replication-factor 2 &>/dev/null; then
    log_info "✓ Test topic '$TEST_TOPIC' created successfully"

    # Test message production
    if echo "Test message $(date)" | kubectl exec -i kafka-0 -n kafka-learning -- kafka-console-producer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$TEST_TOPIC" &>/dev/null; then
        log_info "✓ Message production test passed"
    else
        log_warn "⚠ Message production test failed"
    fi

    # Test message consumption
    if timeout 10 kubectl exec kafka-0 -n kafka-learning -- kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$TEST_TOPIC" \
        --from-beginning \
        --max-messages 1 &>/dev/null; then
        log_info "✓ Message consumption test passed"
    else
        log_warn "⚠ Message consumption test failed"
    fi

    # Cleanup test topic
    kubectl exec kafka-0 -n kafka-learning -- kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --delete \
        --topic "$TEST_TOPIC" &>/dev/null || true
    log_info "✓ Test topic cleaned up"
else
    log_warn "⚠ Test topic creation failed"
fi

# Check monitoring stack
log_info "Validating monitoring stack..."

# Check VictoriaMetrics
vm_ready=$(kubectl get deployment victoriametrics -n kafka-monitoring -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [ "$vm_ready" = "1" ]; then
    log_info "✓ VictoriaMetrics is ready"
else
    log_warn "⚠ VictoriaMetrics is not ready ($vm_ready/1 replicas)"
fi

# Check Grafana
grafana_ready=$(kubectl get deployment grafana -n kafka-monitoring -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [ "$grafana_ready" = "1" ]; then
    log_info "✓ Grafana is ready"
else
    log_warn "⚠ Grafana is not ready ($grafana_ready/1 replicas)"
fi

# Check vmagent
vmagent_ready=$(kubectl get deployment vmagent -n kafka-monitoring -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [ "$vmagent_ready" = "1" ]; then
    log_info "✓ vmagent is ready"
else
    log_warn "⚠ vmagent is not ready ($vmagent_ready/1 replicas)"
fi

# Check kafka-exporter
exporter_ready=$(kubectl get deployment kafka-exporter -n kafka-learning -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [ "$exporter_ready" = "1" ]; then
    log_info "✓ kafka-exporter is ready"
else
    log_warn "⚠ kafka-exporter is not ready ($exporter_ready/1 replicas)"
fi

# Final summary
echo ""
echo "================================"
echo "Validation Summary"
echo "================================"

if [ "$zk_replicas" = "3" ] && [ "$kafka_replicas" = "3" ] && [ "$vm_ready" = "1" ] && [ "$grafana_ready" = "1" ]; then
    log_info "✅ Kafka playground is ready for use!"
    echo ""
    echo "Next steps:"
    echo "1. Access Grafana: kubectl port-forward -n kafka-monitoring svc/grafana 3000:3000"
    echo "   Then visit: http://localhost:3000 (admin/admin)"
    echo "2. Run producer/consumer tests in the scripts/ directory"
    echo "3. Monitor cluster health through Grafana dashboards"
else
    log_warn "⚠️  Some components are not fully ready. Check the logs above."
    echo ""
    echo "Troubleshooting commands:"
    echo "- kubectl get pods -n kafka-learning"
    echo "- kubectl get pods -n kafka-monitoring"
    echo "- kubectl logs -n kafka-learning <pod-name>"
    echo "- kubectl describe pod -n kafka-learning <pod-name>"
fi