Daily process
===

```
mvn clean && mvn compile && mvn package && spark-submit --class process.Process target/HourlyProcess-0.0.1-SNAPSHOT.jar
```

Init tables:

```
CREATE TABLE device_usages (
	id serial PRIMARY KEY NOT NULL,
	device_family text,
	created_at timestamp WITH time ZONE,
	time_usage bigint
)
```