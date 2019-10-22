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
- ssh hadoop@192.168.199.233 (登录到服务器)
- 1),从 ~/software下解压到~/app目录下
- 2),配置系统环境变量, ~/.bash_profile
- 3),配置文件 $ZK_HOME/conf.zoo.cfg  dataDir不要放到默认的/tmp下
- 4),启动ZK  $ZK_HOME/bin/zkServer.sh start
- 5),检查是否启动成功 jps -> QuorumPeerMain