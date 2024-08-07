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

### Consumer Commands
- #### Create a consumer group: `./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --topic <topic-name> --group <group-name>`
- #### List consumer groups: `./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list`
- #### Consume messages from a topic: `./bin/kafka-console-consumer.sh --topic <topic-name> --bootstrap-server localhost:9092 --from-beginning`
- #### Produce messages to a topic: `./bin/kafka-console-producer.sh --topic <topic-name> --bootstrap-server localhost:9092`
- #### Describe consumer group: `./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <group-id>`
- #### Reset consumer group offsets: `./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group-id> --reset-offsets --to-earliest --execute --all-topics`
- #### Reset consumer group offsets for a topic: `./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group-id> --reset-offsets --to-earliest --execute --topic <topic-name>`
- #### Reset consumer group offsets for a topic partition: `./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group-id> --reset-offsets --to-earliest --execute --topic <topic-name> --partition <partition-id>`
- #### Reset consumer group offsets for a topic partition to a specific offset: `./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group-id> --reset-offsets --to-offset <offset> --execute --topic <topic-name> --partition <partition-id>`
- #### Reset consumer group offsets for a topic partition to a specific timestamp: `./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group-id> --reset-offsets --to-datetime <timestamp> --execute --topic <topic-name> --partition <partition-id>`

## Kafka Producer Configuration
- #### Producer Configuration: `./config/producer.properties`
- #### Producer Configuration with SSL: `./config/producer-ssl.properties`
- #### Producer Configuration with SASL_SSL: `./config/producer-sasl-ssl.properties`
- #### Producer Configuration with ACLs: `./config/producer-acls.properties`
- #### Producer Configuration with ACLs and SSL: `./config/producer-acls-ssl.properties`
- #### Producer Configuration with ACLs and SASL_SSL: `./config/producer-acls-sasl-ssl.properties`
- #### Producer Configuration with ACLs and SASL_SSL and SSL: `./config/producer-acls-sasl-ssl-ssl.properties`
- #### Producer Configuration with ACLs and SASL_SSL and SSL and Kerberos: `./config/producer-acls-sasl-ssl-ssl-kerberos.properties`
- #### Producer Configuration with ACLs and SASL_SSL and SSL and Kerberos and ACLs: `./config/producer-acls-sasl-ssl-ssl-kerberos-acls.properties`
- #### Producer Configuration with ACLs and SASL_SSL and SSL and Kerberos and ACLs and SASL_SSL: `./config/producer-acls-sasl-ssl-ssl-kerberos-acls-sasl-ssl.properties`
- #### Producer Configuration with ACLs and SASL_SSL and SSL and Kerberos and ACLs and SASL_SSL and SSL: `./config/producer-acls-sasl-ssl-ssl-kerberos-acls-sasl-ssl-ssl.properties`
- #### Producer Configuration with ACLs and SASL_SSL and SSL and Kerberos and ACLs and SASL_SSL and SSL and ACLs: `./config/producer-acls-sasl-ssl-ssl-kerberos-acls-sasl-ssl-ssl-acls.properties`
- #### Producer Configuration with ACLs and SASL_SSL and SSL and Kerberos and ACLs and SASL_SSL and SSL and ACLs and SASL_SSL: `./config/producer-acls-sasl-ssl-ssl-kerberos-acls-sasl-ssl-ssl-acls-sasl-ssl.properties`
- #### Producer Configuration with ACLs and SASL_SSL and SSL and Kerberos and ACLs and SASL_SSL and SSL and ACLs and SASL_SSL and ACLs: `./config/producer-acls-sasl-ssl-ssl-kerberos-acls-sasl-ssl-ssl-acls-sasl-ssl-acls.properties`
- #### Producer Configuration with ACLs and SASL_SSL and SSL and Kerberos and ACLs and SASL_SSL and SSL and ACLs and SASL_SSL and ACLs and SSL: `./config/producer-acls-sasl-ssl-ssl-kerberos-acls-sasl-ssl-ssl-acls-sasl-ssl-acls-ssl.properties`
- #### Producer Configuration with ACLs and SASL_SSL and SSL and Kerberos and ACLs and SASL_SSL and SSL and ACLs and SASL_SSL and ACLs and SSL and ACLs: `./config/producer-acls-sasl-ssl-ssl-kerberos-acls-sasl-ssl-ssl-acls-sasl-ssl-acls-ssl-acls.properties`

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



