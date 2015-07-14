This repository holds some common gearpump usage patterns with Java. 

The examples include:
* word count -- simple Java app that shows the structure of gearpump app
* Kafka -> Kafka pipeline -- very simple example that shows how to read and write from Kafka topics
* Kafka -> HBase pipeline -- how to read from Kafka topic, how to write to HBase

The following sections will give you information about:
* How to build and run examples
* How specific example works

# Building and running the examples

The repository is organized in one maven project that contains all the examples. 
 

## Build

To build the examples run:

`mvn package`

After build, there is a jar under `target/streaming-java-template-1.1-SNAPSHOT.jar`.

## Running an example

1. Start the gearpump cluster (0.4)

  a) Download from http://www.gearpump.io/site/downloads/

  b) After extraction, start the local cluster
  ```bash
  bin/local
  ```

  c) Start the UI server
  ```bash
  bin/services
  ```

2. Submit the jar
  ```bash
  bin/gear app -jar path/to/streaming-java-template-1.1-SNAPSHOT.jar <app mainclass with package> 
  ```
  
  for example:
  
  ```bash
  bin/gear app -jar target/streaming-java-template-1.1-SNAPSHOT.jar javatemplate.WordCount 
  ```
  
  
3. Check the UI
  http://127.0.0.1:8090/  
  



> NOTE:
> 
> Please use Java7 to run the cluster.
> 
> You can set the ENV JAVA_HOME.

> On windows:
> set JAVA_HOME={path_to_java_7}
> 
> On Linux
> export JAVA_HOME={path_to_java_7}

# Examples description

## kafka2kafka-pipeline
Very simple example that shows how to read and write from Kafka topics.

The example makes use of Gearpump Connector API, `KafkaSource` and `KafkaSink`, that make simple operations with Kafka super easy.


When defining Kafka source, you'll need to provide topic name and zookeeper location:
```Java
KafkaSource kafkaSource = new KafkaSource("inputTopic", "localhost:2181");
```

When defining Kafka sink (output), you will just give the destination topic name and Kafka broker address:
```Java
KafkaSink kafkaSink = new KafkaSink("outputTopic", "localhost:9092");
```

Keep in mind, that Kafka source processor produces message as byte array (`byte[]`). 
Also, Kafka sink procesor expects the message to be scala.Tuple. 

The example shows dedicated steps that do the necessary conversions. 
(The conversions don't need to be a separate step, you could include them in other task that do actual computation.)  

### Dependencies
This example uses zookeeper and Kafka. You need to set them up before running.

Start zookeeper and Kafka:

```bash
zookeeper/bin/zkServer.sh start

kafka/bin/kafka-server-start.sh kafka/config/server.properties
```

(Tested with zookeeper 3.4.6 and Kafka 2.11-0.8.2.1. with default settings.)

The app will read messages from `inputTopic` and write to `outputTopic`, so you need to create them beforehand:

```bash
kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic inputTopic

kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic outputTopic
```

### Testing
After you prepared Kafka topics and deployed the app to gearpump cluster, you can start using it.

Start producing some messages to input topic:

```bash
kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic inputTopic
```


Check if anything appears on output topic:

```bash
kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic outputTopic --from-beginning
```
