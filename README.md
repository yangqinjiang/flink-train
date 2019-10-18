练习Flink计算引擎的例子

## java api quickstart 
`https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/projectsetup/java_api_quickstart.html`

前置条件：  Maven 3.0.4 + 和Java 8.x +
```
# 使用mvn创建flink工程
mvn archetype:generate \
 -DarchetypeGroupId=org.apache.flink \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion=1.7.2 \
  -DarchetypeCatalog=local
```