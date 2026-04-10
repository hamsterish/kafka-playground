#!/bin/bash
# Chaos Engineering: Random Pod Termination
# Educational Focus: Understanding Kafka resilience patterns and recovery mechanisms

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_chaos() {
    echo -e "${CYAN}[CHAOS]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Configuration
KAFKA_NAMESPACE="kafka-learning"
STRIMZI_NAMESPACE="strimzi-kafka"
DEFAULT_CHAOS_DURATION=300  # 5 minutes
DEFAULT_KILL_INTERVAL=30    # 30 seconds between kills

# Help function
show_help() {
    cat << EOF
Kafka Chaos Engineering Tool

USAGE:
    $0 [OPTIONS] [COMMAND]

COMMANDS:
    start-chaos        Start chaos testing (default)
    kill-broker        Kill a random Kafka broker
    kill-zookeeper     Kill a random ZooKeeper pod
    kill-random        Kill any random Kafka/ZK pod
    network-partition  Simulate network partition (requires NetworkPolicy support)
    resource-stress    Apply resource constraints
    stop-chaos         Stop all chaos testing

OPTIONS:
    -d, --duration SECONDS     Chaos test duration (default: 300)
    -i, --interval SECONDS     Time between chaos actions (default: 30)
    -n, --namespace NAMESPACE  Target namespace (default: kafka-learning)
    -t, --type TYPE           Deployment type: manual|strimzi (default: manual)
    -m, --monitor             Enable continuous monitoring during chaos
    --dry-run                 Show what would be done without executing
    -h, --help               Show this help

EXAMPLES:
    # Basic chaos testing for 5 minutes
    $0 start-chaos

    # Extended chaos testing with monitoring
    $0 --duration 600 --monitor start-chaos

    # Target Strimzi deployment
    $0 --type strimzi --namespace strimzi-kafka start-chaos

    # Kill specific component type
    $0 kill-broker
    $0 kill-zookeeper

    # Stress test with resource constraints
    $0 resource-stress

EDUCATIONAL FOCUS:
This tool helps understand:
- Kafka partition leadership election
- ZooKeeper leader election and consensus
- Consumer group rebalancing
- Producer retry and recovery
- Under-replicated partition handling
- Network partition tolerance
EOF
}

# Parse command line arguments
CHAOS_DURATION=$DEFAULT_CHAOS_DURATION
KILL_INTERVAL=$DEFAULT_KILL_INTERVAL
TARGET_NAMESPACE=$KAFKA_NAMESPACE
DEPLOYMENT_TYPE="manual"
ENABLE_MONITORING=false
DRY_RUN=false
COMMAND="start-chaos"

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--duration)
            CHAOS_DURATION="$2"
            shift 2
            ;;
        -i|--interval)
            KILL_INTERVAL="$2"
            shift 2
            ;;
        -n|--namespace)
            TARGET_NAMESPACE="$2"
            shift 2
            ;;
        -t|--type)
            DEPLOYMENT_TYPE="$2"
            if [[ "$DEPLOYMENT_TYPE" == "strimzi" ]]; then
                TARGET_NAMESPACE=$STRIMZI_NAMESPACE
            fi
            shift 2
            ;;
        -m|--monitor)
            ENABLE_MONITORING=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        start-chaos|kill-broker|kill-zookeeper|kill-random|network-partition|resource-stress|stop-chaos)
            COMMAND="$1"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Utility functions
check_prerequisites() {
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed"
        exit 1
    fi

    if ! kubectl get namespace $TARGET_NAMESPACE &> /dev/null; then
        log_error "Namespace $TARGET_NAMESPACE not found"
        exit 1
    fi
}

get_kafka_pods() {
    if [[ "$DEPLOYMENT_TYPE" == "strimzi" ]]; then
        kubectl get pods -n $TARGET_NAMESPACE -l strimzi.io/kind=Kafka -o name | sed 's/pod\///'
    else
        kubectl get pods -n $TARGET_NAMESPACE -l app=kafka -o name | sed 's/pod\///'
    fi
}

get_zookeeper_pods() {
    if [[ "$DEPLOYMENT_TYPE" == "strimzi" ]]; then
        kubectl get pods -n $TARGET_NAMESPACE -l strimzi.io/kind=Kafka,strimzi.io/name=kafka-playground-cluster-zookeeper -o name | sed 's/pod\///'
    else
        kubectl get pods -n $TARGET_NAMESPACE -l app=zookeeper -o name | sed 's/pod\///'
    fi
}

kill_random_pod() {
    local pod_type=$1
    local pods

    case $pod_type in
        "kafka")
            pods=($(get_kafka_pods))
            ;;
        "zookeeper")
            pods=($(get_zookeeper_pods))
            ;;
        "random")
            local kafka_pods=($(get_kafka_pods))
            local zk_pods=($(get_zookeeper_pods))
            pods=("${kafka_pods[@]}" "${zk_pods[@]}")
            ;;
    esac

    if [[ ${#pods[@]} -eq 0 ]]; then
        log_warn "No $pod_type pods found in namespace $TARGET_NAMESPACE"
        return 1
    fi

    # Select random pod
    local random_index=$((RANDOM % ${#pods[@]}))
    local target_pod=${pods[$random_index]}

    log_chaos "🎯 Target selected: $target_pod"

    # Get pod details before termination
    local pod_node=$(kubectl get pod $target_pod -n $TARGET_NAMESPACE -o jsonpath='{.spec.nodeName}')
    local pod_age=$(kubectl get pod $target_pod -n $TARGET_NAMESPACE -o jsonpath='{.status.startTime}')

    log_info "Pod details: node=$pod_node, age=$pod_age"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would delete pod $target_pod"
        return 0
    fi

    # Pre-chaos health check
    log_step "Pre-chaos cluster health check..."
    check_cluster_health

    # Execute chaos action
    log_chaos "💥 Terminating pod: $target_pod"
    kubectl delete pod $target_pod -n $TARGET_NAMESPACE --force --grace-period=0

    # Monitor recovery
    log_step "Monitoring recovery..."
    monitor_pod_recovery $target_pod $pod_type

    # Post-chaos health check
    log_step "Post-chaos cluster health check..."
    check_cluster_health

    return 0
}

check_cluster_health() {
    log_info "🔍 Checking cluster health..."

    # Check pod status
    local unhealthy_pods=$(kubectl get pods -n $TARGET_NAMESPACE --no-headers | grep -v Running | grep -v Completed | wc -l)
    if [[ $unhealthy_pods -gt 0 ]]; then
        log_warn "⚠️  $unhealthy_pods unhealthy pods detected"
        kubectl get pods -n $TARGET_NAMESPACE --no-headers | grep -v Running | grep -v Completed || true
    else
        log_info "✅ All pods are running"
    fi

    # Check under-replicated partitions (if possible)
    check_under_replicated_partitions

    # Check consumer lag (if monitoring is enabled)
    if [[ "$ENABLE_MONITORING" == "true" ]]; then
        check_consumer_lag
    fi
}

check_under_replicated_partitions() {
    # Attempt to check URP via kubectl exec
    local kafka_pods=($(get_kafka_pods))
    if [[ ${#kafka_pods[@]} -gt 0 ]]; then
        local kafka_pod=${kafka_pods[0]}

        # Try to execute URP check
        local urp_count
        if [[ "$DEPLOYMENT_TYPE" == "strimzi" ]]; then
            # Strimzi uses different paths
            urp_count=$(kubectl exec $kafka_pod -n $TARGET_NAMESPACE -- /bin/bash -c \
                "kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions 2>/dev/null | wc -l" || echo "unknown")
        else
            urp_count=$(kubectl exec $kafka_pod -n $TARGET_NAMESPACE -- /bin/bash -c \
                "kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions 2>/dev/null | wc -l" || echo "unknown")
        fi

        if [[ "$urp_count" != "unknown" ]] && [[ "$urp_count" -gt 0 ]]; then
            log_warn "⚠️  Under-replicated partitions detected: $urp_count"
        elif [[ "$urp_count" != "unknown" ]]; then
            log_info "✅ No under-replicated partitions"
        else
            log_warn "❓ Could not check under-replicated partitions"
        fi
    fi
}

check_consumer_lag() {
    log_info "🔍 Checking consumer lag via monitoring..."
    # This would integrate with Prometheus/Grafana
    # For now, just indicate the check
    log_info "📊 Monitor consumer lag at: http://localhost:3000/d/kafka-consumers"
}

monitor_pod_recovery() {
    local killed_pod=$1
    local pod_type=$2
    local start_time=$(date +%s)

    log_step "⏳ Waiting for pod replacement..."

    # Wait for new pod to be scheduled
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        # Check if original pod is gone and new one is running
        if ! kubectl get pod $killed_pod -n $TARGET_NAMESPACE &> /dev/null; then
            log_info "✅ Original pod terminated successfully"

            # Check for replacement pod
            local replacement_pods
            case $pod_type in
                "kafka")
                    replacement_pods=($(get_kafka_pods))
                    ;;
                "zookeeper")
                    replacement_pods=($(get_zookeeper_pods))
                    ;;
            esac

            if [[ ${#replacement_pods[@]} -gt 0 ]]; then
                log_info "✅ Replacement pod(s) found: ${replacement_pods[*]}"
                break
            fi
        fi

        if [[ $elapsed -gt 120 ]]; then
            log_warn "⚠️  Pod recovery taking longer than expected (${elapsed}s)"
        fi

        sleep 5
    done

    local total_recovery_time=$(($(date +%s) - start_time))
    log_info "⏱️  Recovery time: ${total_recovery_time} seconds"
}

simulate_network_partition() {
    log_chaos "🌐 Simulating network partition..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would create NetworkPolicy to simulate partition"
        return 0
    fi

    # Create a temporary NetworkPolicy that blocks traffic to one broker
    local kafka_pods=($(get_kafka_pods))
    if [[ ${#kafka_pods[@]} -eq 0 ]]; then
        log_error "No Kafka pods found for network partition test"
        return 1
    fi

    local target_pod=${kafka_pods[0]}
    log_chaos "🚫 Creating network partition for pod: $target_pod"

    # Create NetworkPolicy (this requires a network plugin that supports NetworkPolicy)
    cat << EOF | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: chaos-network-partition-${target_pod}
  namespace: $TARGET_NAMESPACE
spec:
  podSelector:
    matchLabels:
      app: kafka
  policyTypes:
  - Ingress
  - Egress
  ingress: []
  egress: []
EOF

    log_info "NetworkPolicy created. Partition will last $KILL_INTERVAL seconds..."
    sleep $KILL_INTERVAL

    # Remove NetworkPolicy
    kubectl delete networkpolicy chaos-network-partition-${target_pod} -n $TARGET_NAMESPACE
    log_info "✅ Network partition removed"
}

apply_resource_stress() {
    log_chaos "💾 Applying resource constraints for stress testing..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would apply resource constraints"
        return 0
    fi

    # Apply temporary resource constraints
    local kafka_pods=($(get_kafka_pods))
    for pod in "${kafka_pods[@]}"; do
        log_chaos "⚡ Applying CPU stress to pod: $pod"

        # This is a placeholder - actual implementation would use tools like stress-ng
        kubectl exec $pod -n $TARGET_NAMESPACE -- /bin/bash -c \
            "nohup stress --cpu 2 --timeout ${KILL_INTERVAL}s > /dev/null 2>&1 &" || true
    done

    log_info "Resource stress applied for $KILL_INTERVAL seconds..."
    sleep $KILL_INTERVAL
    log_info "✅ Resource stress completed"
}

start_monitoring() {
    if [[ "$ENABLE_MONITORING" != "true" ]]; then
        return 0
    fi

    log_step "🖥️  Starting monitoring dashboard..."

    # Start port-forward to Grafana in background
    kubectl port-forward -n kafka-monitoring svc/grafana 3000:3000 > /dev/null 2>&1 &
    local pf_pid=$!
    echo $pf_pid > /tmp/chaos-monitoring-pid

    sleep 5
    log_info "📊 Grafana available at: http://localhost:3000"
    log_info "   Username: admin, Password: admin"
    log_info "🎯 Monitor chaos impact on:"
    log_info "   - Kafka Overview Dashboard"
    log_info "   - Consumer Lag Dashboard"
    log_info "   - Under-replicated Partitions"
}

stop_monitoring() {
    if [[ -f /tmp/chaos-monitoring-pid ]]; then
        local pf_pid=$(cat /tmp/chaos-monitoring-pid)
        kill $pf_pid 2>/dev/null || true
        rm -f /tmp/chaos-monitoring-pid
        log_info "🔴 Monitoring stopped"
    fi
}

# Main chaos testing function
start_chaos_testing() {
    local end_time=$(($(date +%s) + CHAOS_DURATION))
    local iteration=1

    log_chaos "🎪 Starting chaos testing for $CHAOS_DURATION seconds..."
    log_info "Target namespace: $TARGET_NAMESPACE ($DEPLOYMENT_TYPE deployment)"
    log_info "Kill interval: $KILL_INTERVAL seconds"

    start_monitoring

    while [[ $(date +%s) -lt $end_time ]]; do
        local remaining_time=$((end_time - $(date +%s)))

        log_step "🎯 Chaos iteration #$iteration (${remaining_time}s remaining)"

        # Randomly choose chaos action
        local chaos_actions=("kafka" "zookeeper" "random")
        local random_action=${chaos_actions[$((RANDOM % ${#chaos_actions[@]}))]}

        kill_random_pod $random_action

        log_info "⏳ Waiting $KILL_INTERVAL seconds before next chaos action..."
        sleep $KILL_INTERVAL

        iteration=$((iteration + 1))
    done

    log_chaos "🏁 Chaos testing completed after $((iteration - 1)) iterations"
    stop_monitoring
}

# Main execution
main() {
    echo "=============================================="
    echo "🎪 Kafka Chaos Engineering Tool"
    echo "Educational Kafka Playground - Phase 4"
    echo "=============================================="
    echo ""

    check_prerequisites

    case $COMMAND in
        "start-chaos")
            start_chaos_testing
            ;;
        "kill-broker")
            kill_random_pod "kafka"
            ;;
        "kill-zookeeper")
            kill_random_pod "zookeeper"
            ;;
        "kill-random")
            kill_random_pod "random"
            ;;
        "network-partition")
            simulate_network_partition
            ;;
        "resource-stress")
            apply_resource_stress
            ;;
        "stop-chaos")
            stop_monitoring
            log_info "Chaos testing stopped"
            ;;
        *)
            log_error "Unknown command: $COMMAND"
            show_help
            exit 1
            ;;
    esac
}

# Trap signals for clean shutdown
trap 'log_warn "Received interrupt signal, cleaning up..."; stop_monitoring; exit 0' INT TERM

# Run main function
main "$@"