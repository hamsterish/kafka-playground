#!/bin/bash
# Strimzi Operator Installation Script
# Educational Focus: Transition from manual deployment to operator-based management

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

    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed or not in PATH"
        exit 1
    fi

    # Check helm
    if ! command -v helm &> /dev/null; then
        log_error "helm is not installed or not in PATH"
        log_info "Please install Helm 3.x from https://helm.sh/docs/intro/install/"
        exit 1
    fi

    # Check cluster connectivity
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi

    # Check helm version
    HELM_VERSION=$(helm version --short | cut -d'+' -f1 | sed 's/v//')
    HELM_MAJOR=$(echo $HELM_VERSION | cut -d'.' -f1)
    if [ "$HELM_MAJOR" -lt 3 ]; then
        log_error "Helm 3.x is required, found version $HELM_VERSION"
        exit 1
    fi

    log_info "✓ Prerequisites check passed"
    log_info "  - kubectl: $(kubectl version --client -o yaml | grep gitVersion | cut -d'"' -f4)"
    log_info "  - helm: v$HELM_VERSION"
}

# Create Strimzi namespace
create_strimzi_namespace() {
    log_step "Creating Strimzi system namespace..."

    kubectl create namespace strimzi-system --dry-run=client -o yaml | kubectl apply -f -
    kubectl label namespace strimzi-system name=strimzi-system --overwrite

    log_info "✓ Strimzi system namespace ready"
}

# Add Strimzi Helm repository
add_helm_repository() {
    log_step "Adding Strimzi Helm repository..."

    # Add the Strimzi Helm repository
    helm repo add strimzi https://strimzi.io/charts/

    # Update repository information
    helm repo update

    log_info "✓ Strimzi Helm repository added and updated"
}

# Install Strimzi operator
install_strimzi_operator() {
    log_step "Installing Strimzi Kafka operator..."

    # Check if already installed
    if helm list -n strimzi-system | grep -q strimzi-kafka-operator; then
        log_warn "Strimzi operator is already installed, upgrading..."
        INSTALL_CMD="upgrade"
    else
        INSTALL_CMD="install"
    fi

    # Install or upgrade Strimzi operator
    helm $INSTALL_CMD strimzi-kafka-operator strimzi/strimzi-kafka-operator \
        --namespace strimzi-system \
        --version 0.40.0 \
        --set watchAnyNamespace=true \
        --set defaultImageRegistry=quay.io \
        --set defaultImageRepository=strimzi \
        --set image.registry=quay.io \
        --set image.repository=strimzi/operator \
        --set image.name=operator \
        --set logLevel=INFO \
        --set fullReconciliationIntervalMs=120000 \
        --set operationTimeoutMs=300000 \
        --wait \
        --timeout=600s

    log_info "✓ Strimzi operator installed successfully"
}

# Wait for operator to be ready
wait_for_operator() {
    log_step "Waiting for Strimzi operator to be ready..."

    # Wait for deployment to be available
    kubectl wait --for=condition=available deployment/strimzi-cluster-operator \
        -n strimzi-system \
        --timeout=300s

    log_info "✓ Strimzi cluster operator is ready"
}

# Verify CRDs installation
verify_crds() {
    log_step "Verifying Strimzi Custom Resource Definitions..."

    EXPECTED_CRDS=(
        "kafkas.kafka.strimzi.io"
        "kafkatopics.kafka.strimzi.io"
        "kafkausers.kafka.strimzi.io"
        "kafkaconnects.kafka.strimzi.io"
        "kafkamirrormakers.kafka.strimzi.io"
        "kafkabridges.kafka.strimzi.io"
        "kafkarebalances.kafka.strimzi.io"
    )

    for crd in "${EXPECTED_CRDS[@]}"; do
        if kubectl get crd "$crd" &>/dev/null; then
            log_info "✓ CRD found: $crd"
        else
            log_warn "⚠ CRD missing: $crd"
        fi
    done

    # Count total Strimzi CRDs
    CRD_COUNT=$(kubectl get crd | grep strimzi.io | wc -l)
    log_info "✓ Total Strimzi CRDs installed: $CRD_COUNT"
}

# Create Strimzi namespace for Kafka cluster (separate from operator)
create_kafka_namespace() {
    log_step "Creating Kafka cluster namespace..."

    kubectl create namespace strimzi-kafka --dry-run=client -o yaml | kubectl apply -f -
    kubectl label namespace strimzi-kafka name=strimzi-kafka --overwrite

    log_info "✓ Strimzi Kafka namespace ready"
}

# Display installation summary
show_installation_summary() {
    log_step "Strimzi Installation Complete!"
    echo ""
    echo "=========================================="
    echo "Strimzi Kafka Operator Installation Summary"
    echo "=========================================="
    echo ""
    echo "🎯 Operator Details:"
    echo "   Namespace: strimzi-system"
    echo "   Version: 0.40.0"
    echo "   Watch Mode: All namespaces"
    echo ""
    echo "🔍 Monitoring Commands:"
    echo "   kubectl get pods -n strimzi-system"
    echo "   kubectl logs -n strimzi-system deployment/strimzi-cluster-operator -f"
    echo "   kubectl get crd | grep strimzi"
    echo ""
    echo "📋 Available Custom Resources:"
    echo "   kubectl get kafka -A"
    echo "   kubectl get kafkatopic -A"
    echo "   kubectl get kafkauser -A"
    echo ""
    echo "🚀 Next Steps:"
    echo "   1. Deploy Kafka cluster: kubectl apply -f kafka-cluster.yaml"
    echo "   2. Create topics: kubectl apply -f kafka-topic.yaml"
    echo "   3. Monitor cluster: kubectl get kafka -n strimzi-kafka -w"
    echo ""
    echo "📖 Educational Comparison:"
    echo "   Compare the Strimzi CRDs with manual k8s-manual/ deployments"
    echo "   Notice the simplified configuration and built-in best practices"
    echo "   Observe automatic rolling updates and configuration validation"
    echo ""
}

# Main execution
main() {
    echo "=============================================="
    echo "Strimzi Kafka Operator Installation"
    echo "Educational Kafka Playground - Phase 3"
    echo "=============================================="
    echo ""

    check_prerequisites
    create_strimzi_namespace
    add_helm_repository
    install_strimzi_operator
    wait_for_operator
    verify_crds
    create_kafka_namespace
    show_installation_summary
}

# Error handling
trap 'log_error "Strimzi installation failed. Check the logs above."' ERR

# Run main function
main "$@"