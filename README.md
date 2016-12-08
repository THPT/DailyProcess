Daily process
===

```
mvn clean && mvn compile && mvn package && spark-submit --class process.Process target/HourlyProcess-0.0.1-SNAPSHOT.jar
```