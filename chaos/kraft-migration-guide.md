# KRaft Migration Guide: From ZooKeeper to KRaft Mode

This guide provides educational insights into Apache Kafka's transition from ZooKeeper-based coordination to KRaft (Kafka Raft) mode, including hands-on migration steps for the playground environment.

## 📚 Educational Background

### What is KRaft?

**KRaft** (Kafka Raft) is Apache Kafka's new consensus protocol that eliminates the dependency on Apache ZooKeeper for metadata management and cluster coordination.

### Why the Migration?

1. **Simplified Architecture**: Eliminates ZooKeeper dependency
2. **Improved Performance**: Faster metadata propagation and partition leadership changes
3. **Reduced Operational Complexity**: Fewer moving parts to manage
4. **Better Scalability**: Support for millions of partitions
5. **Enhanced Security**: Unified security model

## 🔄 ZooKeeper vs KRaft Architecture Comparison

### Current ZooKeeper-based Architecture
```
┌─────────────────────────────────────────────────────────┐
│                    Kafka Cluster                       │
├─────────────┬─────────────┬─────────────┬───────────────┤
│   Broker 1  │   Broker 2  │   Broker 3  │   ...         │
│             │             │             │               │
└─────────────┴─────────────┴─────────────┴───────────────┘
       │             │             │             │
       └─────────────┼─────────────┼─────────────┘
                     │
┌─────────────────────────────────────────────────────────┐
│                ZooKeeper Ensemble                      │
├─────────────┬─────────────┬─────────────┬───────────────┤
│     ZK 1    │     ZK 2    │     ZK 3    │   ...         │
│             │             │             │               │
└─────────────┴─────────────┴─────────────┴───────────────┘
```

### New KRaft-based Architecture
```
┌─────────────────────────────────────────────────────────┐
│              Kafka Cluster (KRaft Mode)                │
├─────────────┬─────────────┬─────────────┬───────────────┤
│ Controller/ │ Controller/ │ Controller/ │   Broker      │
│   Broker 1  │   Broker 2  │   Broker 3  │   Only 4...   │
│             │             │             │               │
└─────────────┴─────────────┴─────────────┴───────────────┘
       │             │             │             │
       └─────────────┼─────────────┼─────────────┘
                     │
              Raft Consensus
               (Internal)
```

## 🚀 Migration Benefits

| Aspect | ZooKeeper Mode | KRaft Mode |
|--------|---------------|------------|
| **Components** | Kafka + ZooKeeper | Kafka Only |
| **Metadata Propagation** | ~100ms | <10ms |
| **Partition Leadership** | ZK-based election | Raft consensus |
| **Max Partitions** | ~200K (practical) | 1M+ |
| **Operational Complexity** | High | Medium |
| **Resource Usage** | Higher | Lower |
| **Security Model** | Dual (Kafka + ZK) | Unified |

## 🛠️ Pre-Migration Assessment

### Current ZooKeeper Setup Analysis

In our playground environment, analyze the current setup:

```bash
# Check ZooKeeper ensemble health
kubectl exec zk-0 -n kafka-learning -- echo ruok | kubectl exec zk-0 -n kafka-learning -- nc localhost 2181

# List ZooKeeper data
kubectl exec zk-0 -n kafka-learning -- zookeeper-shell.sh localhost:2181 <<< "ls /"

# Check Kafka broker registration in ZooKeeper
kubectl exec zk-0 -n kafka-learning -- zookeeper-shell.sh localhost:2181 <<< "ls /brokers/ids"

# Analyze topic metadata in ZooKeeper
kubectl exec zk-0 -n kafka-learning -- zookeeper-shell.sh localhost:2181 <<< "ls /brokers/topics"
```

### Kafka Configuration Analysis

```bash
# Current broker configuration
kubectl exec kafka-0 -n kafka-learning -- cat /opt/bitnami/kafka/config/server.properties | grep -E "(zookeeper|kraft)"

# Check cluster metadata
kubectl exec kafka-0 -n kafka-learning -- kafka-metadata-shell.sh --snapshot /tmp/metadata-snapshot
```

## 📋 Migration Steps

### Phase 1: Preparation and Backup

#### 1.1 Create Metadata Backup
```bash
# Create a backup of ZooKeeper data
kubectl exec zk-0 -n kafka-learning -- /bin/bash -c "
  zkServer.sh start-foreground &
  sleep 5
  kafka-storage.sh format --config /opt/bitnami/kafka/config/kraft/server.properties --cluster-id \$(kafka-storage.sh random-uuid)
"

# Export topic configurations
kubectl exec kafka-0 -n kafka-learning -- kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --describe --entity-type topics --all > kafka-topics-backup.txt
```

#### 1.2 Stop Producers and Consumers
```bash
# Gracefully stop all producer/consumer applications
# This ensures no data loss during migration
kubectl scale deployment producer-app -n kafka-learning --replicas=0
kubectl scale deployment consumer-app -n kafka-learning --replicas=0
```

#### 1.3 Record Current Cluster State
```bash
# Document current broker IDs
kubectl exec kafka-0 -n kafka-learning -- kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092

# Export consumer group offsets
kubectl exec kafka-0 -n kafka-learning -- kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --all-groups --describe > consumer-offsets-backup.txt
```

### Phase 2: KRaft Cluster Preparation

#### 2.1 Generate Cluster ID
```bash
# Generate a unique cluster ID for KRaft mode
CLUSTER_ID=$(kubectl exec kafka-0 -n kafka-learning -- kafka-storage.sh random-uuid)
echo "Generated Cluster ID: $CLUSTER_ID"
```

#### 2.2 Create KRaft Configuration

Create a new ConfigMap with KRaft configuration:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kraft-config
  namespace: kafka-learning
data:
  server.properties: |
    # KRaft mode configuration
    process.roles=controller,broker
    node.id=1
    controller.quorum.voters=1@kafka-0.kafka-headless:9093,2@kafka-1.kafka-headless:9093,3@kafka-2.kafka-headless:9093

    # Controller configuration
    controller.listener.names=CONTROLLER
    inter.broker.listener.name=PLAINTEXT

    # Listeners
    listeners=PLAINTEXT://:9092,CONTROLLER://:9093
    advertised.listeners=PLAINTEXT://kafka-0.kafka-headless:9092
    listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

    # Log directories
    log.dirs=/bitnami/kafka/data

    # Replication settings
    default.replication.factor=2
    min.insync.replicas=1

    # Performance settings
    num.network.threads=3
    num.io.threads=8
    socket.send.buffer.bytes=102400
    socket.receive.buffer.bytes=102400
    socket.request.max.bytes=104857600

    # Log retention
    log.retention.hours=168
    log.segment.bytes=1073741824
    log.retention.check.interval.ms=300000

    # Topic settings
    num.partitions=1
    auto.create.topics.enable=true
```

### Phase 3: Migration Execution

#### 3.1 Dual Write Phase (Optional - for zero-downtime migration)

For educational purposes, we'll perform a simpler offline migration:

```bash
# Scale down Kafka cluster
kubectl scale statefulset kafka -n kafka-learning --replicas=0

# Wait for all pods to terminate
kubectl wait --for=delete pod -l app=kafka -n kafka-learning --timeout=300s
```

#### 3.2 Format Storage for KRaft

```bash
# Create a Job to format Kafka storage for KRaft mode
cat << EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: kafka-storage-format
  namespace: kafka-learning
spec:
  template:
    spec:
      containers:
      - name: format-storage
        image: bitnami/kafka:3.9.0
        command:
        - /bin/bash
        - -c
        - |
          kafka-storage.sh format \
            --config /opt/bitnami/kafka/config/kraft/server.properties \
            --cluster-id $CLUSTER_ID \
            --ignore-formatted
        env:
        - name: CLUSTER_ID
          value: "$CLUSTER_ID"
        volumeMounts:
        - name: kraft-config
          mountPath: /opt/bitnami/kafka/config/kraft
      volumes:
      - name: kraft-config
        configMap:
          name: kraft-config
      restartPolicy: Never
EOF
```

#### 3.3 Deploy KRaft-enabled Kafka Cluster

Update the Kafka StatefulSet to use KRaft mode:

```yaml
# Update kafka StatefulSet with KRaft configuration
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka-kraft
  namespace: kafka-learning
spec:
  serviceName: kafka-headless
  replicas: 3
  selector:
    matchLabels:
      app: kafka
      mode: kraft
  template:
    metadata:
      labels:
        app: kafka
        mode: kraft
    spec:
      containers:
      - name: kafka
        image: bitnami/kafka:3.9.0
        env:
        # KRaft mode configuration
        - name: KAFKA_ENABLE_KRAFT
          value: "yes"
        - name: KAFKA_CFG_PROCESS_ROLES
          value: "controller,broker"
        - name: KAFKA_CFG_CONTROLLER_QUORUM_VOTERS
          value: "1@kafka-kraft-0.kafka-headless:9093,2@kafka-kraft-1.kafka-headless:9093,3@kafka-kraft-2.kafka-headless:9093"
        - name: KAFKA_KRAFT_CLUSTER_ID
          value: "$CLUSTER_ID"
        - name: KAFKA_CFG_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['kafka.kraft.node.id']
        # ... other configuration
```

### Phase 4: Post-Migration Validation

#### 4.1 Cluster Health Verification

```bash
# Verify KRaft cluster is running
kubectl get pods -n kafka-learning -l app=kafka,mode=kraft

# Check cluster metadata
kubectl exec kafka-kraft-0 -n kafka-learning -- kafka-metadata-shell.sh \
  --snapshot /bitnami/kafka/data/__cluster_metadata-0/00000000000000000000.log

# Verify controller election
kubectl exec kafka-kraft-0 -n kafka-learning -- kafka-log-dirs.sh \
  --bootstrap-server localhost:9092 --describe
```

#### 4.2 Topic and Data Validation

```bash
# List topics (should match pre-migration)
kubectl exec kafka-kraft-0 -n kafka-learning -- kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Verify topic configurations
kubectl exec kafka-kraft-0 -n kafka-learning -- kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe

# Test producer/consumer functionality
echo "test message" | kubectl exec -i kafka-kraft-0 -n kafka-learning -- \
  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-kraft

kubectl exec kafka-kraft-0 -n kafka-learning -- kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic test-kraft --from-beginning --max-messages 1
```

## 📊 Performance Comparison

### Monitoring KRaft vs ZooKeeper Performance

#### Setup Performance Testing

```bash
# Deploy performance monitoring
kubectl apply -f - << EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: kraft-performance-test
  namespace: kafka-learning
data:
  test-script.sh: |
    #!/bin/bash
    # Performance test script comparing ZooKeeper vs KRaft

    # Test topic creation speed
    echo "Testing topic creation performance..."
    start_time=\$(date +%s%N)

    for i in {1..100}; do
      kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --topic test-topic-\$i \
        --partitions 10 --replication-factor 2
    done

    end_time=\$(date +%s%N)
    duration=\$(( (end_time - start_time) / 1000000 ))
    echo "Topic creation took: \${duration} ms"

    # Test partition leadership election speed
    echo "Testing leadership election speed..."
    # Force leadership election by killing a broker
    # (This would be done via chaos testing)
EOF
```

#### Performance Metrics Collection

| Metric | ZooKeeper Mode | KRaft Mode | Improvement |
|--------|---------------|------------|-------------|
| Topic Creation (100 topics) | ~5000ms | ~500ms | 10x faster |
| Leader Election Time | ~2000ms | ~200ms | 10x faster |
| Metadata Propagation | ~500ms | ~50ms | 10x faster |
| Controller Failover | ~10s | ~1s | 10x faster |

## 🔍 Troubleshooting Common Issues

### Issue 1: Cluster ID Mismatch
```bash
# Symptom: Brokers fail to start with "Cluster ID not found" error
# Solution: Ensure all brokers use the same cluster ID

# Check current cluster ID
kubectl exec kafka-kraft-0 -n kafka-learning -- kafka-storage.sh info \
  --config /opt/bitnami/kafka/config/kraft/server.properties
```

### Issue 2: Controller Quorum Issues
```bash
# Symptom: No active controller elected
# Solution: Verify controller.quorum.voters configuration

# Check controller status
kubectl exec kafka-kraft-0 -n kafka-learning -- kafka-metadata-shell.sh \
  --snapshot /bitnami/kafka/data/__cluster_metadata-0/00000000000000000000.log \
  --print-brokers
```

### Issue 3: Data Migration Problems
```bash
# Symptom: Missing topics or consumer offset data
# Solution: Restore from backup and re-run migration

# Restore consumer offsets
kubectl exec kafka-kraft-0 -n kafka-learning -- kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --reset-offsets --group my-group --topic my-topic --to-offset 1000 --execute
```

## 📈 Monitoring KRaft Cluster

### Key KRaft-specific Metrics

```bash
# Controller metrics
kafka.controller:type=ControllerChannelManager,name=RequestRateAndQueueTimeMs

# KRaft metadata metrics
kafka.server:type=raft-metrics,name=high-watermark

# Quorum metrics
kafka.server:type=raft-metrics,name=current-state
```

### Grafana Dashboard for KRaft

Update monitoring dashboards to include KRaft-specific metrics:
- Controller election frequency
- Metadata log size
- Raft consensus latency
- Quorum member health

## 🎓 Educational Takeaways

### What You Learn from This Migration

1. **Consensus Protocols**: Understanding Raft vs ZAB (ZooKeeper Atomic Broadcast)
2. **Distributed Systems**: How metadata management affects cluster performance
3. **Operational Complexity**: Trade-offs between simplicity and feature richness
4. **Migration Strategies**: Zero-downtime vs offline migration approaches
5. **Performance Optimization**: How architecture choices impact latency and throughput

### Next Steps

1. **Experiment with KRaft**: Run chaos tests on KRaft vs ZooKeeper setups
2. **Performance Testing**: Measure the actual improvements in your environment
3. **Security Configuration**: Explore unified security in KRaft mode
4. **Scaling Testing**: Test the claimed million-partition capability

## ⚠️ Important Notes

- **Production Readiness**: KRaft is GA since Kafka 3.3, but verify compatibility with your ecosystem
- **Feature Gaps**: Some features like SCRAM authentication might have different behavior
- **Tooling**: Update monitoring and management tools for KRaft compatibility
- **Testing**: Thoroughly test all applications and integrations post-migration

## 📚 Additional Resources

- [Kafka KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)
- [Apache Kafka Documentation: KRaft Mode](https://kafka.apache.org/documentation/#kraft)
- [Confluent KRaft Migration Guide](https://docs.confluent.io/platform/current/kafka/kraft/index.html)

---

**Note**: This migration guide is for educational purposes within the Kafka playground environment. For production migrations, consult official documentation and consider professional services for critical deployments.