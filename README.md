# streaming
Ingest real-time data from Kafka to Hive via Storm.

## Modules
* Kafka Producer: Ingest real-time Twitter data to Kafka in JSON format
* Storm Topology: Read Kafka topics with TwitterBolt and Write to Hive by using HiveBolt

Step 1: Execute runTwitterKafkaProducer.sh to start TwitterKafkaProducer.

## Build or Download the JSON SerDe

Source: https://github.com/rcongiu/Hive-JSON-Serde

```
ADD JAR /root/Hive-Json-Serde/json-serde-1.3.8-SNAPSHOT-jar-with-dependencies.jar;

CREATE EXTERNAL TABLE tweets (
  id BIGINT,
  created_at STRING,
  source STRING,
  favorited BOOLEAN,
  retweeted_status STRUCT<
    text:STRING,
    user:STRUCT<screen_name:STRING,name:STRING>,
    retweet_count:INT>,
  entities STRUCT<
    urls:ARRAY<STRUCT<expanded_url:STRING>>,
    user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
    hashtags:ARRAY<STRUCT<text:STRING>>>,
  text STRING,
  user STRUCT<
    screen_name:STRING,
    name:STRING,
    friends_count:INT,
    followers_count:INT,
    statuses_count:INT,
    verified:BOOLEAN,
    utc_offset:INT,
    time_zone:STRING>,
  in_reply_to_screen_name STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/root/tweets';
```
