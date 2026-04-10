# Kafka Playground: A Progressive Learning Environment

A comprehensive educational project for mastering Apache Kafka 3.9 in Kubernetes, progressing from manual infrastructure to modern operator-based management with comprehensive observability.

## 🎯 Learning Objectives

- **Kafka Fundamentals**: Deep understanding of brokers, topics, partitions, and replication
- **ZooKeeper Integration**: Service discovery, leader election, and configuration management
- **Kubernetes StatefulSets**: Persistent storage, ordered deployment, and stable network identities
- **Observability**: Metrics collection, alerting, and visualization with VictoriaMetrics/Grafana
- **Operator Benefits**: Migration from manual deployments to Strimzi operator management
- **Chaos Engineering**: Resilience testing and failure recovery patterns

## 📁 Project Structure

```
kafka-playground/
├── README.md                          # This file - master documentation
├── k8s-manual/                        # Phase 1: Manual Kubernetes deployments
├── monitoring/                        # Comprehensive observability stack
├── scripts/                           # Phase 2: Data simulation and load testing
├── k8s-strimzi/                      # Phase 3: Strimzi operator migration
├── chaos/                            # Phase 4: Resilience testing
└── tests/                            # Integration and unit tests
```

## 🚀 Implementation Phases

### Phase 1: Manual Foundation + Observability
Deploy Kafka 3.9 and ZooKeeper using raw Kubernetes manifests with comprehensive monitoring:
- 3-node ZooKeeper ensemble
- 3 Kafka brokers with JMX metrics
- VictoriaMetrics for metrics storage
- Grafana with pre-built Kafka dashboards
- Kafka and ZooKeeper exporters

### Phase 2: Data Pipeline & Load Testing
Build realistic data simulation with monitoring:
- Python producer with temperature sensor data
- Consumer with windowed aggregation
- Kubernetes Job for scale testing
- Custom metrics integration

### Phase 3: Strimzi Operator Evolution
Migrate to modern operator-based management:
- Helm-based Strimzi installation
- CRD-driven cluster configuration
- Enhanced built-in observability
- Operational best practices comparison

### Phase 4: Chaos Engineering
Validate cluster resilience with visual monitoring:
- Automated failure injection
- Real-time recovery monitoring
- KRaft migration preparation
- Performance impact analysis

## 🔧 Prerequisites

- Local Kubernetes cluster (minikube, kind, or k3s)
- kubectl configured
- Docker for container builds
- Python 3.9+ for client applications
- Helm 3.x for operator installation

## 📊 Monitoring Stack

- **VictoriaMetrics**: High-performance metrics storage
- **Grafana**: Visualization with Kafka-specific dashboards
- **Kafka Exporter**: JMX to Prometheus metrics conversion
- **vmagent**: Metrics collection and forwarding

## 🎓 Educational Focus

This project emphasizes deep understanding over quick deployment:
- Detailed comments explaining every configuration choice
- Comparison between manual and operator approaches
- Hands-on chaos testing with visual feedback
- Progressive complexity with clear learning checkpoints

## 🏁 Getting Started

1. Clone this repository
2. Ensure you have a running Kubernetes cluster
3. Start with Phase 1: `kubectl apply -f monitoring/` then `kubectl apply -f k8s-manual/`
4. Access Grafana at `http://localhost:3000` (after port-forwarding)
5. Progress through each phase systematically

## 🧪 Testing Philosophy

- Unit tests for all Python components
- Integration tests for data pipeline
- Chaos tests for resilience validation
- Performance tests for load characteristics

---

**Note**: This is an educational project focusing on deep Kafka and Kubernetes understanding. Each phase builds upon previous knowledge, so complete them in sequence.