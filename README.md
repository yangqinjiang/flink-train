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
- 1),从 ~/software下解压到~/app目录下,tar -zxvf zookeeper-3.4.10.tar.gz -C ~/app
- 2),配置系统环境变量, ~/.bash_profile,并且source ~/.bash_profile
- 3),配置文件 $ZK_HOME/conf/zoo.cfg  dataDir不要放到默认的/tmp下
- 4),启动ZK  $ZK_HOME/bin/zkServer.sh start
- 5),检查是否启动成功 jps -> QuorumPeerMain

#Kafka的安装
- 下载地址: http://kafka.apache.org/downloads , https://archive.apache.org/dist/kafka/1.1.0/kafka_2.11-1.1.0.tgz
- 解压 : tar -zxvf kafka_2.11-1.1.0.tgz -C ~/app
- 配置系统环境变量, ~/.bash_profile,并且source ~/.bash_profile
- 修改config/server.properties,修改两处: log.dirs=/home/hadoop/app/tmp , zookeeper.connect=hadoop000:2181
- 启动: bin/kafka-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties
- 检查是否启动成功 jps -> Kafka
- 测试kafka,创建topic: bin/kafka-topics.sh --create --zookeeper hadoop000:2181 --replication-factor 1 --partitions 1 --topic pktest
- 查看所有的topic:  bin/kafka-topics.sh --list --zookeeper hadoop000:2181
- 启动生产者: bin/kafka-console-producer.sh --broker-list hadoop000:9092 --topic pktest
- 启动消费者: bin/kafka-console-consumer.sh --bootstrap-server hadoop000:9092 --topic pktest --from-beginning


#flink的编译,安装,前置条件,JDK8,MVN3.3.9
- 下载地址: https://codeload.github.com/apache/flink/tar.gz/release-1.7.0
- 解压: tar -zxvf flink-release-1.7.0.tar.gz -C ~/app
- mvn clean install -DskipTests -Pvendor-repos -Dfast -DHadoop.version=2.6.0-cdh5.7.0
- 将花很长时间来编译,包括下载jar包
- 编译好的结果在 flink-dist

#基于Flink的互联网直播平台日志分析项目实战
```
日志格式:
    aliyun
    CN
    E
    [17/Jul/2018:17:07:50 +0800]
    223.104.18.110
    v2.go2yd.com
    17168
```
接入的数据类型就是日志
- 离线: Flume ==> HDFS
- 实时: Kafka ==> 流处理引擎 ==> ES ==> Kibana

项目功能
- 1,统计一分钟内每个域名访问产生的流量
    `Flink接收Kafka的进行处理`
- 2, 统计一分钟内每个用户产生的流量
    `域名和用户是有对应关系的`
    `Flink接收Kafka的进行 + Flink读取域名和用户的配置数据  进行处理`
    
    
# (需求一)ElasticSearch 的使用
curl -XPUT 'http://hadoop000:9200/cdn'

curl -H "Content-Type: application/json" -XPOST 'http://hadoop000:9200/cdn/traffic/_mapping'
{
"traffic":{
    "properties":{
        "domain":{"type":"keyword"},
        "traffics":{"type":"long"},
        "time":{"type":"date","format":"yyyy-MM-dd HH:mm"}
    }
}
}

#安装kibana
- 设置浏览器的时区: management -> Kibana:Advanced Settings -> Timezone for date formatting : Etc/GMT


#
```  
domains:
    v1.go2yd.com
    v2.go2yd.com
    v3.go2yd.com
    v4.go2yd.com
    vmi.go2yd.com
userid: 8000001
    v1.go2yd.com
```  
用户id和域名的映射关系
    从日志里能拿到domain,还得从另外一个表(MySQL)里面去获取userid和domain的映射关系
```  
CREATE TABLE user_domain_config(
id int unsigned auto_increment,
user_id varchar(40) not null,
domain varchar(40) not null,
primary key (id)
)

INSERT INTO user_domain_config (user_id,domain) values ('8000000','v1.go2yd.com');
INSERT INTO user_domain_config (user_id,domain) values ('8000001','v2.go2yd.com');
INSERT INTO user_domain_config (user_id,domain) values ('8000000','v3.go2yd.com');
INSERT INTO user_domain_config (user_id,domain) values ('8000002','v4.go2yd.com');
INSERT INTO user_domain_config (user_id,domain) values ('8000000','vmi.go2yd.com');
```  
在做实时数据清洗的时候, 不仅需要处理原始日志,还需要关联MySQL表里的数据
自定义一个Flink去读取MySQL数据的数据源,然后把两个Stream关联起来

# (需求二)ElasticSearch 的使用
curl -XPUT 'http://hadoop000:9200/cdn2'

curl -H "Content-Type: application/json" -XPOST 'http://hadoop000:9200/cdn2/traffic/_mapping'
{
"traffic":{
    "properties":{
        "domain":{"type":"keyword"},
        "traffics":{"type":"long"},
        "time":{"type":"date","format":"yyyy-MM-dd HH:mm"},
        "userid":{"type":"keyword"}
    }
}
}


#Flink进行数据的清洗
- 读取kafka的数据
- 读取MySQL的数据
- connect
  业务逻辑的处理分析, 水印 windowFunction
   ==> ES 注意数据类型  <== kibana 图形化的统计结果展示
   
- kibana :各个环节的监控,监控图形化,
1 30
2 40
3 300
4 35 
数据的波动,便于观察
我们已经实现的 + CDN业务文档的描述  => 扩展
---
# 单机模式部署及代码提交测试
`./bin/start-cluster.sh  # Start Flink`

`jps 查看输出,应该有这两个进程: StandaloneSessionClusterEntrypoint,TaskManagerRunner`
## 查看运行日志
`cd log/ ,有相关的日志  `
## 运行示例程序
```
1,先启动nc
nc -L -p 9000
2,提交flink示例程序
# 流处理
./bin/flink run examples/streaming/SocketWindowWordCount.jar --port 9000
# 批处理
 .\bin\flink run .\examples\batch\WordCount.jar -input /path/to/input.txt -output /path/to/output.txt
```
# 关闭单机模式的部署
`./bin/stop-cluster.sh # stop Flink`

# Flink部署及作业提交
`https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/yarn_setup.html`
ON YARN是企业级用的最多的方式(推荐)

# FLINK ON YARN 第一种模式实践
## Start a long-running Flink cluster on YARN (作为长时间运行的job,运行在YARN上)
./bin/yarn-session.sh -n 1 -jm 1024m -tm 1024m
### 参数说明 ,可参考 yarn-session.sh --help
- -n taskmanager的数量
- -jm jobmanager的内存
- -tm taskmanager的内存

### 提交作业
```
#下载等待统计的文本
wget -O LICENSE-2.0.txt http://www.apache.org/licenses/LICENSE-2.0.txt
#提交 示例
./bin/flink run ./examples/batch/WordCount.jar \
-input hdfs://hadoop000:8020/LICENSE-2.0.txt \
-output hdfs://hadoop000:8020/wordcount-result.txt \
#查看结果
hdfs fs -text /wordcount-result.txt
```
### 杀死 flink job ON YARN
`yarn application -kill application_xxxxxxxxx`
`jps , 杀死 FlinkYarnSessionCli 进程`
---
# FLINK ON YARN 第二种模式实践
## Run a single Flink job on YARN
`参考文档:https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/yarn_setup.html#run-a-single-flink-job-on-yarn`
#### 示例
`./bin/flink run -m yarn-cluster ./examples/batch/WordCount.jar`
### 查看flink job 运行日志
`yarn logs -applicationId <application ID>`