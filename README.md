练习Flink计算引擎的例子

## java api quickstart 
`https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/projectsetup/java_api_quickstart.html`

前置条件：  Maven 3.0.4 + 和Java 8.x +
# 使用mvn创建flink-java工程
```
mvn archetype:generate \
 -DarchetypeGroupId=org.apache.flink \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion=1.7.2 \
  -DarchetypeCatalog=local
```
# 使用mvn创建flink-scala工程
```
mvn archetype:generate \
 -DarchetypeGroupId=org.apache.flink \
  -DarchetypeArtifactId=flink-quickstart-scala \
  -DarchetypeVersion=1.7.2 \
  -DarchetypeCatalog=local
```


#Watermark的说明
`https://blog.csdn.net/lmalds/article/details/52704170`

#ZooKeeper的安装
- 下载地址: https://archive.cloudera.com/cdh5/cdh/5/
-  或者: https://archive.apache.org/dist/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.tar.gz
- ssh hadoop@192.168.199.233 (登录到服务器)
- 1),从 ~/software下解压到~/app目录下
- 2),配置系统环境变量, ~/.bash_profile,并且source ~/.bash_profile
- 3),配置文件 $ZK_HOME/conf.zoo.cfg  dataDir不要放到默认的/tmp下
- 4),启动ZK  $ZK_HOME/bin/zkServer.sh start
- 5),检查是否启动成功 jps -> QuorumPeerMain

#Kafka的安装
- 下载地址: http://kafka.apache.org/downloads , https://archive.apache.org/dist/kafka/1.1.0/kafka_2.11-1.1.0.tgz
- 解压 : tar -zxvf kafka_2.11-1.1.0.tgz -C ~/app
- 配置系统环境变量, ~/.bash_profile,并且source ~/.bash_profile
- 修改config/server.properties,修改两处: log.dirs=/home/hadoop/app/tmp , zookeeper.connect=hadoop000:2181
- 启动: bin/kakfa-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties
- 检查是否启动成功 jps -> Kafka
- 测试kafka,创建topic: bin/kafka-topics.sh --create --zookeeper hadoop000:2181 --replication-factor 1 --partitions 1 --topic pktest
- 查看所有的topic:  bin/kafka-topics.sh --list --zookeeper hadoop000:2181
- 启动生产者: bin/kafka-console-producer.sh --broker-list hadoop000:9092 --topic pktest
- 启动消费者: bin/kafka-console-consumer.sh --bootstrap-server hadoop000:9092 --topic pktest --from-beginning