## Feature
1. kafka leader election
## Requirement
jdk1.8+
## Run
```
cd /data/bigdata/confluent-3.2.0
# copy KafkaTools-0.1.0.jar to ./
java -jar KafkaTools-0.1.0.jar
```
## Commands
```
KafkaTools=>help
AVAILABLE COMMANDS

Built-In Commands
        clear: Clear the shell screen.
        exit, quit: Exit the shell.
        help: Display help about available commands.
        script: Read and execute commands from a file.
        stacktrace: Display the full stacktrace of the last error.

Kafka Leader Election
        backup-leader-info: backup-leader-info --broker 10.16.238.101:8092
        cancel-broker-leader: cancel-broker-leader --broker 10.16.238.101:8092 --zookeeper 10.16.238.101:8181 --broker-id 0 --kafka-install-home /data/bigdata/confluent-3.2.0
        cancel-broker-leader-by-topic: cancel-broker-leader-by-topic --broker 10.16.238.101:8092 --zookeeper 10.16.238.101:8181 --broker-id 0 --topic trumantest --kafka-install-home /data/bigdata/confluent-3.2.0
        generate-topics: generate-topics --zookeeper 10.16.238.101:8181
        reassign-partitions: reassign-partitions --broker 10.16.238.101:8092 --broker-id 0
        reassign-partitions-by-topic: reassign-partitions-by-topic --broker 10.16.238.101:8092 --broker-id 0 --topic kafkatopic
        reassign-partitions-by-topics: reassign-partitions-by-topics --broker 10.16.238.101:8092 --broker-id 0 --topics-json-file topics.json

```
## Tips
- backup-leader-info 备份当前leader分布情况，主要在backup目录下生成文件
- cancel-broker-leader 下线所有topic 指定的leader
- cancel-broker-leader-by-topic 下线指定topic 指定的leader
- generate-topics 生成集群中所有topic的json文件
- reassign-partitions 自动获取所有topics,然后根据该topics,brokerId取出相关partitions，将该信息中的Replicas中指定brokerId移至数组列尾，仅生成配置文件
- reassign-partitions-by-topic 根据topic与brokerId取出相关partitions，将该信息中的Replicas中指定brokerId移至数组列尾，仅生成配置文件
- reassign-partitions-by-topics 根据topicsJson与brokerId取出相关partitions，将该信息中的Replicas中指定brokerId移至数组列尾，仅生成配置文件
