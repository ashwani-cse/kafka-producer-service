# kafka-producer-service
The producer microservice publishes events/messages to Kafka topics for event-driven communication.

## Kafka Commands
- #### Start Zookeeper: `./bin/zookeeper-server-start.sh ./config/zookeeper.properties`
- #### Start Kafka Broker: `./bin/kafka-server-start.sh ./config/server.properties`
- #### if your are in /bin dir already, then you can run below command
- #### Start Zookeeper: `./zookeeper-server-start.sh ../config/zookeeper.properties`
- #### Start Kafka Broker: `./kafka-server-start.sh ../config/server.properties`

### Open Kafka Broker bash
- #### Open Kafka Producer bash to publish messages: `./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic <topic-name>`
- #### Open Kafka Consumer bash to consume messages: `./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic-name> --from-beginning (optional)`

### Topic Commands
- #### List all topics: `./bin/kafka-topics.sh --list --bootstrap-server localhost:9092`
- #### Create a topic: `./bin/kafka-topics.sh --create --topic <topic-name> --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`
- #### Describe a topic: `./bin/kafka-topics.sh --describe --topic <topic-name> --bootstrap-server localhost:9092`
- #### Delete a topic: `./bin/kafka-topics.sh --delete --topic <topic-name> --bootstrap-server localhost:9092`
- #### Reset offsets for a topic: `./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group-id> --reset-offsets --to-earliest --execute --topic <topic-name>`
- #### Reset offsets for all topics: `./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group-id> --reset-offsets --to-earliest --execute --all-topics`

### Console Producer commands
- #### Publish messages to a topic: `./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic <topic-name>`
- #### Publish messages to a topic with key: `./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic <topic-name> --property "parse.key=true" --property "key.separator=:"`

### Console Consumer commands
- #### Consume messages from a topic: `./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic-name> --from-beginning`
- #### Consume messages from a topic with key: `./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic-name> --from-beginning --property "print.key=true" --property "key.separator=:"`
- #### Consume messages from a topic with group: `./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic-name> --group <group-id>`

## Help
### If you see below error on zookeeper starts:-
```
[2024-07-26 17:42:20,745] ERROR Unexpected exception, exiting abnormally (org.apache.zookeeper.server.ZooKeeperServerMain)
java.io.IOException: No snapshot found, but there are log entries. Something is broken!
	at org.apache.zookeeper.server.persistence.FileTxnSnapLog.restore(FileTxnSnapLog.java:290)
	at org.apache.zookeeper.server.ZKDatabase.loadDataBase(ZKDatabase.java:285)
	at org.apache.zookeeper.server.ZooKeeperServer.loadData(ZooKeeperServer.java:531)
	at org.apache.zookeeper.server.ZooKeeperServer.startdata(ZooKeeperServer.java:704)
	at org.apache.zookeeper.server.NIOServerCnxnFactory.startup(NIOServerCnxnFactory.java:744)
	at org.apache.zookeeper.server.ServerCnxnFactory.startup(ServerCnxnFactory.java:130)
	at org.apache.zookeeper.server.ZooKeeperServerMain.runFromConfig(ZooKeeperServerMain.java:161)
	at org.apache.zookeeper.server.ZooKeeperServerMain.initializeAndRun(ZooKeeperServerMain.java:113)
	at org.apache.zookeeper.server.ZooKeeperServerMain.main(ZooKeeperServerMain.java:68)
	at org.apache.zookeeper.server.quorum.QuorumPeerMain.initializeAndRun(QuorumPeerMain.java:141)
	at org.apache.zookeeper.server.quorum.QuorumPeerMain.main(QuorumPeerMain.java:91)
[2024-07-26 17:42:20,761] INFO ZooKeeper audit is disabled. (org.apache.zookeeper.audit.ZKAuditProvider)
[2024-07-26 17:42:20,768] ERROR Exiting JVM with code 1 (org.apache.zookeeper.util.ServiceUtils)
```

### Solution
- #### Delete the data directory of zookeeper and restart zookeeper
```shell
rm -rf /tmp/zookeeper/version-2
#OR
rm -rf zookeeper
```

### On Mac, If unable to find /tmp directory
- #### 1. On Terminal, run below command to find the TMPDIR
```shell
cd ~/ 
cd /tmp/
```
- #### 2.  Now, you can see the zookeeper & kafka directory and delete zookeeper dir
```shell
i. Open Finder
ii. Press Command + Shift + G
iii. Paste the path /tmp
``` 

# Examples
- ### How to create consumer with group and consumer id
```shell
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic t2 --group t2_group --consumer-property group.instance.id=consumer_1
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic t2 --group t2_group --consumer-property group.instance.id=consumer_2
```
- ### Descibe the group t2_group
```shell
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group t2_group
```
- #### Output: 
```shell
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                     HOST            CLIENT-ID
t2_group        t2              0          0               0               0               consumer_1-5aae467d-1f6a-4eb1-bfa4-a58db1a3ef56  /127.0.0.1     console-consumer
t2_group        t2              1          0               0               0               consumer_1-5aae467d-1f6a-4eb1-bfa4-a58db1a3ef56  /127.0.0.1     console-consumer   
t2_group        t2              2          0               0               0               consumer_2-2cdbff41-b99a-435b-94a5-4b8285e2d62d  /127.0.0.1     console-consumer
```
## Kafka’s Automatic Partition Assignment using Round-Robin Algorithm

### Partition Distribution
- **Automatic Assignment:** Kafka balances partitions across consumers in a group.
- **Excess Consumers:** If consumers > partitions, some consumers will be idle.
- **Fewer Consumers:** If consumers < partitions, some consumers handle multiple partitions.

### Rebalancing
- **Joining/Leaving Consumers:** Kafka triggers a rebalance when consumers join or leave.
- **Session Timeouts:** Failing to send heartbeats within the session timeout triggers a rebalance.

### Scaling
- **Adding Consumers:** More consumers than partitions will not increase consumption but allows for future scalability.
- **Removing Consumers:** Fewer consumers will handle more partitions until a rebalance occurs.

## Stateful operations in Kafka Streams
- **Stateful Operations:** Operations that require maintaining state across multiple records in a stream.
- **State Store:** The state is typically stored in state stores, which can be backed by a local storage or a distributed database like RocksDB.
- **Fault Tolerance:** Kafka Streams provides fault tolerance by backing up state stores to Kafka topics.
- **Recovery:** When a Kafka Streams application restarts, it can restore the state from the backup topics.
- **State Store:** The state store is partitioned to allow for parallel processing and scalability.
### Stateful Operations
- **Aggregations:** Combining multiple records into a single record. Examples include sum, count, average, and min/max.
- **Joins:** Combining records from multiple streams based on a common key. Examples include inner join, left join, and outer join.
- **Windowed Operations:** Grouping records in certain time windows. Examples calculate total number of orders in the last 5 minutes.

#### How Aggregations work in Kafka Streams
- Aggregations works only on Kafka records that have non-null keys.
- **Operations:**
- **Count:** Counts the number of records.
- **Reduce:** Combines records using a custom reducer.
- **Aggregate:** Combines records using a custom aggregator.

## Repartitioning in Kafka Streams

Repartitioning is a critical operation in Kafka Streams, ensuring that records with the same key are processed in the same partition. However, it can be resource-intensive, so it's important to understand and optimize its usage.

### When Does Repartitioning Occur?

- **Grouping:** Repartitioning happens when records are grouped by a new key using `groupBy()`.
- **Joining:** During join operations, repartitioning aligns records with the same key from different streams or tables.
- **Windowing:** Repartitioning ensures that windowed records with the same key are processed together.

### Impact of Repartitioning

- **Processing Overhead:** Repartitioning involves writing to a new topic and reading from it, increasing I/O operations, latency, and resource usage.

### Avoid Repartitioning
- **Optimize Key Selection:** Choose keys that minimize repartitioning, such as keys with high cardinality.
- **Use Local State:** When possible, use local state stores to avoid repartitioning.
- **Avoid Unnecessary Operations:** Minimize operations that trigger repartitioning, such as unnecessary groupings or joins.
- **Partitioning Strategy:** Use a custom partitioner to control how records are distributed across partitions.
- **Repartitioning Strategies:** Use strategies like co-partitioning to minimize repartitioning during joins.
- **State Store:** Use RocksDB as a state store to avoid repartitioning for stateful operations.
- **Performance Monitoring:** Monitor repartitioning operations to identify bottlenecks and optimize performance.
- **Scaling:** Scale your Kafka Streams application to distribute the workload and reduce the impact of repartitioning.
- **Testing:** Test your application with different partitioning strategies and configurations to optimize performance.
- **Benchmarking:** Benchmark your application to measure the impact of repartitioning on latency, throughput, and resource usage.
- **Tuning:** Fine-tune your Kafka Streams application based on performance metrics to optimize repartitioning and overall performance.
- **Optimization:** Continuously optimize your application to reduce repartitioning overhead and improve performance.







