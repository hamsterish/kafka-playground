#!/bin/bash
# Monitoring Stack Installation Script
# Automated setup of VictoriaMetrics, Grafana, and vmagent

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_step "Checking prerequisites..."

    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed or not in PATH"
        exit 1
    fi

    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi

    log_info "✓ Prerequisites check passed"
}

# Deploy monitoring namespace
deploy_namespace() {
    log_step "Deploying monitoring namespace..."
    kubectl apply -f 00-monitoring-namespace.yaml
    log_info "✓ Monitoring namespace created"
}

# Deploy VictoriaMetrics
deploy_victoriametrics() {
    log_step "Deploying VictoriaMetrics..."
    kubectl apply -f 01-victoriametrics.yaml

    log_info "Waiting for VictoriaMetrics to become ready..."
    kubectl wait --for=condition=available deployment/victoriametrics -n kafka-monitoring --timeout=300s
    log_info "✓ VictoriaMetrics is ready"
}

# Deploy Grafana
deploy_grafana() {
    log_step "Deploying Grafana..."
    kubectl apply -f 02-grafana.yaml

    log_info "Waiting for Grafana to become ready..."
    kubectl wait --for=condition=available deployment/grafana -n kafka-monitoring --timeout=300s
    log_info "✓ Grafana is ready"
}

# Deploy vmagent
deploy_vmagent() {
    log_step "Deploying vmagent..."
    kubectl apply -f 03-vmagent.yaml

    log_info "Waiting for vmagent to become ready..."
    kubectl wait --for=condition=available deployment/vmagent -n kafka-monitoring --timeout=300s
    log_info "✓ vmagent is ready"
}

# Setup Grafana dashboards
setup_dashboards() {
    log_step "Setting up Grafana dashboards..."

    # Wait for Grafana to be fully ready
    sleep 30

    # Port forward to Grafana (background)
    kubectl port-forward -n kafka-monitoring svc/grafana 3000:3000 &
    PF_PID=$!

    # Wait for port forward to be ready
    sleep 10

    # Import dashboard using API (placeholder - would require curl commands)
    log_info "Dashboard import requires manual step:"
    log_info "1. Access Grafana at http://localhost:3000"
    log_info "2. Login with admin/admin"
    log_info "3. Import dashboards from the dashboards/ directory"

    # Clean up port forward
    kill $PF_PID 2>/dev/null || true
}

# Display access information
show_access_info() {
    log_step "Installation complete!"
    echo ""
    echo "================================"
    echo "Access Information"
    echo "================================"
    echo ""
    echo "🔍 Grafana Dashboard:"
    echo "   kubectl port-forward -n kafka-monitoring svc/grafana 3000:3000"
    echo "   Access: http://localhost:3000"
    echo "   Credentials: admin/admin"
    echo ""
    echo "📊 VictoriaMetrics:"
    echo "   kubectl port-forward -n kafka-monitoring svc/victoriametrics 8428:8428"
    echo "   Access: http://localhost:8428"
    echo ""
    echo "🔧 Useful Commands:"
    echo "   kubectl get pods -n kafka-monitoring"
    echo "   kubectl logs -n kafka-monitoring -l app=vmagent -f"
    echo "   kubectl logs -n kafka-monitoring -l app=grafana -f"
    echo ""
    echo "📈 Next Steps:"
    echo "   1. Deploy Kafka cluster: kubectl apply -f ../k8s-manual/"
    echo "   2. Run validation: ../k8s-manual/validate-manual.sh"
    echo "   3. Start data simulation: python ../scripts/producer.py"
    echo ""
}

# Main execution
main() {
    echo "======================================"
    echo "Kafka Playground Monitoring Setup"
    echo "======================================"
    echo ""

    check_prerequisites
    deploy_namespace
    deploy_victoriametrics
    deploy_grafana
    deploy_vmagent
    setup_dashboards
    show_access_info
}

# Error handling
trap 'log_error "Installation failed. Check the logs above."' ERR

# Run main function
main