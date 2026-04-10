#!/bin/bash
# Kafka Cluster Health Monitor with Grafana Integration
# Educational Focus: Real-time cluster health monitoring during chaos testing

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
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

log_health() {
    echo -e "${CYAN}[HEALTH]${NC} $1"
}

log_metric() {
    echo -e "${MAGENTA}[METRIC]${NC} $1"
}

# Configuration
KAFKA_NAMESPACE="kafka-learning"
STRIMZI_NAMESPACE="strimzi-kafka"
MONITORING_NAMESPACE="kafka-monitoring"
DEFAULT_MONITOR_INTERVAL=10
DEFAULT_ALERT_THRESHOLD_URP=0
DEFAULT_ALERT_THRESHOLD_LAG=1000

# Health check configuration
HEALTH_CHECKS=(
    "pod_status"
    "broker_connectivity"
    "under_replicated_partitions"
    "consumer_lag"
    "topic_health"
    "zookeeper_health"
)

# Help function
show_help() {
    cat << EOF
Kafka Cluster Health Monitor

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -n, --namespace NAMESPACE     Target namespace (default: kafka-learning)
    -t, --type TYPE              Deployment type: manual|strimzi (default: manual)
    -i, --interval SECONDS       Monitoring interval (default: 10)
    -d, --duration SECONDS       Monitor for specific duration (default: infinite)
    --urp-threshold NUM          Alert on URP count > threshold (default: 0)
    --lag-threshold NUM          Alert on consumer lag > threshold (default: 1000)
    --grafana-url URL            Grafana dashboard URL (default: auto-detect)
    --export-metrics             Export metrics to file
    --continuous                 Run continuously until interrupted
    --once                       Run health checks once and exit
    -h, --help                   Show this help

EXAMPLES:
    # Basic continuous monitoring
    $0 --continuous

    # Monitor during chaos testing with alerts
    $0 --interval 5 --urp-threshold 1 --lag-threshold 500

    # Monitor Strimzi deployment
    $0 --type strimzi --namespace strimzi-kafka

    # Single health check
    $0 --once

    # Export metrics for analysis
    $0 --export-metrics --duration 300

EDUCATIONAL FOCUS:
This tool demonstrates:
- Real-time cluster health assessment
- Key Kafka operational metrics
- Integration with Grafana dashboards
- Alerting on operational thresholds
- Performance impact measurement
EOF
}

# Parse command line arguments
TARGET_NAMESPACE=$KAFKA_NAMESPACE
DEPLOYMENT_TYPE="manual"
MONITOR_INTERVAL=$DEFAULT_MONITOR_INTERVAL
MONITOR_DURATION=""
URP_THRESHOLD=$DEFAULT_ALERT_THRESHOLD_URP
LAG_THRESHOLD=$DEFAULT_ALERT_THRESHOLD_LAG
GRAFANA_URL=""
EXPORT_METRICS=false
CONTINUOUS_MODE=false
ONCE_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
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
        -i|--interval)
            MONITOR_INTERVAL="$2"
            shift 2
            ;;
        -d|--duration)
            MONITOR_DURATION="$2"
            shift 2
            ;;
        --urp-threshold)
            URP_THRESHOLD="$2"
            shift 2
            ;;
        --lag-threshold)
            LAG_THRESHOLD="$2"
            shift 2
            ;;
        --grafana-url)
            GRAFANA_URL="$2"
            shift 2
            ;;
        --export-metrics)
            EXPORT_METRICS=true
            shift
            ;;
        --continuous)
            CONTINUOUS_MODE=true
            shift
            ;;
        --once)
            ONCE_MODE=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Global variables for metrics tracking
declare -A METRICS
METRICS_FILE=""
MONITORING_START_TIME=$(date +%s)

# Initialize metrics file if exporting
init_metrics_export() {
    if [[ "$EXPORT_METRICS" == "true" ]]; then
        METRICS_FILE="kafka-health-metrics-$(date +%Y%m%d-%H%M%S).csv"
        echo "timestamp,total_pods,healthy_pods,unhealthy_pods,kafka_pods_ready,zk_pods_ready,urp_count,avg_consumer_lag,brokers_online" > "$METRICS_FILE"
        log_info "📊 Exporting metrics to: $METRICS_FILE"
    fi
}

# Export metrics to file
export_metrics() {
    if [[ "$EXPORT_METRICS" == "true" ]] && [[ -n "$METRICS_FILE" ]]; then
        local timestamp=$(date +%Y-%m-%d\ %H:%M:%S)
        echo "${timestamp},${METRICS[total_pods]},${METRICS[healthy_pods]},${METRICS[unhealthy_pods]},${METRICS[kafka_pods_ready]},${METRICS[zk_pods_ready]},${METRICS[urp_count]},${METRICS[avg_consumer_lag]},${METRICS[brokers_online]}" >> "$METRICS_FILE"
    fi
}

# Check prerequisites
check_prerequisites() {
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed"
        exit 1
    fi

    if ! kubectl get namespace $TARGET_NAMESPACE &> /dev/null; then
        log_error "Target namespace $TARGET_NAMESPACE not found"
        exit 1
    fi
}

# Pod status health check
check_pod_status() {
    log_health "🔍 Checking pod status..."

    local all_pods=$(kubectl get pods -n $TARGET_NAMESPACE --no-headers | wc -l)
    local healthy_pods=$(kubectl get pods -n $TARGET_NAMESPACE --no-headers | grep Running | wc -l)
    local unhealthy_pods=$((all_pods - healthy_pods))

    METRICS[total_pods]=$all_pods
    METRICS[healthy_pods]=$healthy_pods
    METRICS[unhealthy_pods]=$unhealthy_pods

    if [[ $unhealthy_pods -eq 0 ]]; then
        log_info "✅ All pods healthy: $healthy_pods/$all_pods running"
    else
        log_warn "⚠️  Unhealthy pods detected: $unhealthy_pods/$all_pods"
        kubectl get pods -n $TARGET_NAMESPACE --no-headers | grep -v Running | grep -v Completed || true
    fi

    # Specific component checks
    local kafka_pods_ready=0
    local zk_pods_ready=0

    if [[ "$DEPLOYMENT_TYPE" == "strimzi" ]]; then
        kafka_pods_ready=$(kubectl get pods -n $TARGET_NAMESPACE -l strimzi.io/kind=Kafka --no-headers | grep Running | wc -l)
        zk_pods_ready=$(kubectl get pods -n $TARGET_NAMESPACE -l strimzi.io/kind=Kafka,strimzi.io/name=kafka-playground-cluster-zookeeper --no-headers | grep Running | wc -l)
    else
        kafka_pods_ready=$(kubectl get pods -n $TARGET_NAMESPACE -l app=kafka --no-headers | grep Running | wc -l)
        zk_pods_ready=$(kubectl get pods -n $TARGET_NAMESPACE -l app=zookeeper --no-headers | grep Running | wc -l)
    fi

    METRICS[kafka_pods_ready]=$kafka_pods_ready
    METRICS[zk_pods_ready]=$zk_pods_ready

    log_metric "Kafka brokers ready: $kafka_pods_ready"
    log_metric "ZooKeeper pods ready: $zk_pods_ready"
}

# Broker connectivity check
check_broker_connectivity() {
    log_health "🔗 Checking broker connectivity..."

    local kafka_pods
    if [[ "$DEPLOYMENT_TYPE" == "strimzi" ]]; then
        kafka_pods=($(kubectl get pods -n $TARGET_NAMESPACE -l strimzi.io/kind=Kafka -o name | sed 's/pod\///'))
    else
        kafka_pods=($(kubectl get pods -n $TARGET_NAMESPACE -l app=kafka -o name | sed 's/pod\///'))
    fi

    local brokers_online=0
    for pod in "${kafka_pods[@]}"; do
        if kubectl exec $pod -n $TARGET_NAMESPACE -- kafka-broker-api-versions.sh --bootstrap-server localhost:9092 &> /dev/null; then
            brokers_online=$((brokers_online + 1))
        else
            log_warn "⚠️  Broker $pod is not responding"
        fi
    done

    METRICS[brokers_online]=$brokers_online

    if [[ $brokers_online -eq ${#kafka_pods[@]} ]]; then
        log_info "✅ All brokers online: $brokers_online/${#kafka_pods[@]}"
    else
        log_warn "⚠️  Some brokers offline: $brokers_online/${#kafka_pods[@]} responding"
    fi
}

# Under-replicated partitions check
check_under_replicated_partitions() {
    log_health "📊 Checking under-replicated partitions..."

    local kafka_pods
    if [[ "$DEPLOYMENT_TYPE" == "strimzi" ]]; then
        kafka_pods=($(kubectl get pods -n $TARGET_NAMESPACE -l strimzi.io/kind=Kafka -o name | sed 's/pod\///'))
    else
        kafka_pods=($(kubectl get pods -n $TARGET_NAMESPACE -l app=kafka -o name | sed 's/pod\///'))
    fi

    if [[ ${#kafka_pods[@]} -eq 0 ]]; then
        log_warn "No Kafka pods found for URP check"
        METRICS[urp_count]=0
        return
    fi

    local kafka_pod=${kafka_pods[0]}
    local urp_count

    # Try to get URP count
    if kubectl exec $kafka_pod -n $TARGET_NAMESPACE -- /bin/bash -c "command -v kafka-topics.sh" &> /dev/null; then
        urp_count=$(kubectl exec $kafka_pod -n $TARGET_NAMESPACE -- /bin/bash -c \
            "kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions 2>/dev/null | grep -v '^$' | wc -l" 2>/dev/null || echo "0")
    else
        log_warn "kafka-topics.sh not available, skipping URP check"
        urp_count=0
    fi

    METRICS[urp_count]=$urp_count

    if [[ $urp_count -gt $URP_THRESHOLD ]]; then
        log_warn "🚨 ALERT: Under-replicated partitions: $urp_count (threshold: $URP_THRESHOLD)"

        # Show details of under-replicated partitions
        kubectl exec $kafka_pod -n $TARGET_NAMESPACE -- /bin/bash -c \
            "kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions 2>/dev/null" || true
    else
        log_info "✅ Under-replicated partitions: $urp_count"
    fi
}

# Consumer lag check
check_consumer_lag() {
    log_health "📈 Checking consumer lag..."

    # This is a simplified check - in production, you'd query Kafka directly
    # or use monitoring tools like Kafka Manager/CMAK

    local kafka_pods
    if [[ "$DEPLOYMENT_TYPE" == "strimzi" ]]; then
        kafka_pods=($(kubectl get pods -n $TARGET_NAMESPACE -l strimzi.io/kind=Kafka -o name | sed 's/pod\///'))
    else
        kafka_pods=($(kubectl get pods -n $TARGET_NAMESPACE -l app=kafka -o name | sed 's/pod\///'))
    fi

    if [[ ${#kafka_pods[@]} -eq 0 ]]; then
        log_warn "No Kafka pods found for consumer lag check"
        METRICS[avg_consumer_lag]=0
        return
    fi

    local kafka_pod=${kafka_pods[0]}
    local consumer_groups

    # List consumer groups
    if kubectl exec $kafka_pod -n $TARGET_NAMESPACE -- /bin/bash -c "command -v kafka-consumer-groups.sh" &> /dev/null; then
        consumer_groups=$(kubectl exec $kafka_pod -n $TARGET_NAMESPACE -- /bin/bash -c \
            "kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list 2>/dev/null" 2>/dev/null || echo "")
    else
        log_warn "kafka-consumer-groups.sh not available, skipping consumer lag check"
        METRICS[avg_consumer_lag]=0
        return
    fi

    if [[ -z "$consumer_groups" ]]; then
        log_info "✅ No active consumer groups found"
        METRICS[avg_consumer_lag]=0
        return
    fi

    local max_lag=0
    for group in $consumer_groups; do
        if [[ "$group" != "" ]]; then
            local group_lag=$(kubectl exec $kafka_pod -n $TARGET_NAMESPACE -- /bin/bash -c \
                "kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $group 2>/dev/null | awk 'NR>1 {if(\$5 != \"-\") sum+=\$5} END {print sum+0}'" 2>/dev/null || echo "0")

            if [[ $group_lag -gt $max_lag ]]; then
                max_lag=$group_lag
            fi

            if [[ $group_lag -gt $LAG_THRESHOLD ]]; then
                log_warn "⚠️  High consumer lag in group '$group': $group_lag messages"
            fi
        fi
    done

    METRICS[avg_consumer_lag]=$max_lag

    if [[ $max_lag -gt $LAG_THRESHOLD ]]; then
        log_warn "🚨 ALERT: Maximum consumer lag: $max_lag (threshold: $LAG_THRESHOLD)"
    else
        log_info "✅ Consumer lag within threshold: $max_lag messages"
    fi
}

# Topic health check
check_topic_health() {
    log_health "📋 Checking topic health..."

    local kafka_pods
    if [[ "$DEPLOYMENT_TYPE" == "strimzi" ]]; then
        kafka_pods=($(kubectl get pods -n $TARGET_NAMESPACE -l strimzi.io/kind=Kafka -o name | sed 's/pod\///'))
    else
        kafka_pods=($(kubectl get pods -n $TARGET_NAMESPACE -l app=kafka -o name | sed 's/pod\///'))
    fi

    if [[ ${#kafka_pods[@]} -eq 0 ]]; then
        log_warn "No Kafka pods found for topic health check"
        return
    fi

    local kafka_pod=${kafka_pods[0]}
    local topic_count

    if kubectl exec $kafka_pod -n $TARGET_NAMESPACE -- /bin/bash -c "command -v kafka-topics.sh" &> /dev/null; then
        topic_count=$(kubectl exec $kafka_pod -n $TARGET_NAMESPACE -- /bin/bash -c \
            "kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | wc -l" 2>/dev/null || echo "0")
    else
        log_warn "kafka-topics.sh not available, skipping topic health check"
        return
    fi

    log_metric "Total topics: $topic_count"

    # List topics for visibility
    local topics=$(kubectl exec $kafka_pod -n $TARGET_NAMESPACE -- /bin/bash -c \
        "kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null" 2>/dev/null | head -10 || true)

    if [[ -n "$topics" ]]; then
        log_info "📝 Topics (showing first 10):"
        echo "$topics" | while read -r topic; do
            if [[ -n "$topic" ]]; then
                log_info "  - $topic"
            fi
        done
    fi
}

# ZooKeeper health check
check_zookeeper_health() {
    log_health "🐘 Checking ZooKeeper health..."

    local zk_pods
    if [[ "$DEPLOYMENT_TYPE" == "strimzi" ]]; then
        zk_pods=($(kubectl get pods -n $TARGET_NAMESPACE -l strimzi.io/kind=Kafka,strimzi.io/name=kafka-playground-cluster-zookeeper -o name | sed 's/pod\///'))
    else
        zk_pods=($(kubectl get pods -n $TARGET_NAMESPACE -l app=zookeeper -o name | sed 's/pod\///'))
    fi

    local healthy_zk=0
    for pod in "${zk_pods[@]}"; do
        # ZooKeeper health check using echo ruok | nc localhost 2181
        if kubectl exec $pod -n $TARGET_NAMESPACE -- /bin/bash -c "echo ruok | nc localhost 2181 | grep -q imok" 2>/dev/null; then
            healthy_zk=$((healthy_zk + 1))
        else
            log_warn "⚠️  ZooKeeper $pod is not responding to health check"
        fi
    done

    if [[ $healthy_zk -eq ${#zk_pods[@]} ]]; then
        log_info "✅ ZooKeeper ensemble healthy: $healthy_zk/${#zk_pods[@]}"
    else
        log_warn "⚠️  ZooKeeper ensemble issue: $healthy_zk/${#zk_pods[@]} responding"
    fi
}

# Run all health checks
run_health_checks() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo ""
    log_health "🏥 === Health Check Report - $timestamp ==="

    # Initialize metrics
    for key in total_pods healthy_pods unhealthy_pods kafka_pods_ready zk_pods_ready urp_count avg_consumer_lag brokers_online; do
        METRICS[$key]=0
    done

    # Run all health checks
    for check in "${HEALTH_CHECKS[@]}"; do
        case $check in
            "pod_status")
                check_pod_status
                ;;
            "broker_connectivity")
                check_broker_connectivity
                ;;
            "under_replicated_partitions")
                check_under_replicated_partitions
                ;;
            "consumer_lag")
                check_consumer_lag
                ;;
            "topic_health")
                check_topic_health
                ;;
            "zookeeper_health")
                check_zookeeper_health
                ;;
        esac
    done

    # Export metrics if enabled
    export_metrics

    # Show Grafana integration info
    show_grafana_info

    log_health "🏥 === Health Check Complete ==="
    echo ""
}

# Show Grafana dashboard information
show_grafana_info() {
    if kubectl get service grafana -n $MONITORING_NAMESPACE &> /dev/null; then
        log_info "📊 Grafana Dashboards Available:"
        log_info "   kubectl port-forward -n $MONITORING_NAMESPACE svc/grafana 3000:3000"
        log_info "   http://localhost:3000 (admin/admin)"
        log_info "   - Kafka Overview Dashboard"
        log_info "   - Consumer Lag Dashboard"
        log_info "   - Under-replicated Partitions"
    fi
}

# Main monitoring loop
start_monitoring() {
    local start_time=$(date +%s)
    local end_time=""

    if [[ -n "$MONITOR_DURATION" ]]; then
        end_time=$((start_time + MONITOR_DURATION))
        log_info "🕐 Monitoring for $MONITOR_DURATION seconds..."
    else
        log_info "🕐 Continuous monitoring started (Ctrl+C to stop)..."
    fi

    init_metrics_export

    local iteration=1
    while true; do
        # Check if duration limit reached
        if [[ -n "$end_time" ]] && [[ $(date +%s) -ge $end_time ]]; then
            log_info "⏰ Monitoring duration completed"
            break
        fi

        log_info "🔄 Health check iteration #$iteration"
        run_health_checks

        # Exit if in once mode
        if [[ "$ONCE_MODE" == "true" ]]; then
            break
        fi

        log_info "⏳ Next check in $MONITOR_INTERVAL seconds..."
        sleep $MONITOR_INTERVAL
        iteration=$((iteration + 1))
    done

    if [[ "$EXPORT_METRICS" == "true" ]]; then
        log_info "📊 Metrics exported to: $METRICS_FILE"
        log_info "💡 Analyze with: head -n 5 $METRICS_FILE"
    fi
}

# Main execution
main() {
    echo "=============================================="
    echo "🏥 Kafka Cluster Health Monitor"
    echo "Educational Kafka Playground - Monitoring"
    echo "=============================================="
    echo ""

    check_prerequisites

    log_info "🎯 Monitoring configuration:"
    log_info "   Namespace: $TARGET_NAMESPACE"
    log_info "   Deployment: $DEPLOYMENT_TYPE"
    log_info "   Interval: ${MONITOR_INTERVAL}s"
    log_info "   URP Threshold: $URP_THRESHOLD"
    log_info "   Lag Threshold: $LAG_THRESHOLD"
    echo ""

    if [[ "$CONTINUOUS_MODE" == "true" ]] || [[ "$ONCE_MODE" == "true" ]] || [[ -n "$MONITOR_DURATION" ]]; then
        start_monitoring
    else
        # Default: run once
        run_health_checks
    fi
}

# Trap signals for clean shutdown
trap 'log_info "Received interrupt signal, stopping monitoring..."; exit 0' INT TERM

# Run main function
main "$@"