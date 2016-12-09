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

```
CREATE TABLE device_sellings (
	id serial PRIMARY KEY NOT NULL,
	device_family text,
	product_id text,
	created_at timestamp WITH time ZONE,
	amount bigint
);
```

```
CREATE TABLE video_sellings (
	id serial PRIMARY KEY NOT NULL,
	video_id text,
	amount BIGINT,
	created_at timestamp WITH time ZONE
);
```

```
CREATE TABLE referrer_sellings (
	id serial PRIMARY KEY NOT NULL,
	referrer text,
	product_id text,
	amount bigint,
	created_at timestamp WITH time ZONE
);
```
