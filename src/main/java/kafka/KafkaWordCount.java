/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package kafka;

import io.gearpump.cluster.UserConfig;
import io.gearpump.cluster.client.ClientContext;
import io.gearpump.partitioner.HashPartitioner;
import io.gearpump.partitioner.Partitioner;
import io.gearpump.partitioner.ShufflePartitioner;
import io.gearpump.streaming.javaapi.Graph;
import io.gearpump.streaming.javaapi.Processor;
import io.gearpump.streaming.javaapi.StreamApplication;
import io.gearpump.streaming.kafka.KafkaSource;
import io.gearpump.streaming.kafka.KafkaStorageFactory;

public class KafkaWordCount {

  public static void main(String[] args) {

    ClientContext context = ClientContext.apply();

    UserConfig appConfig = UserConfig.empty();

    // we will create two kafka reader task to read from kafka queue.
    int sourceNum = 2;
    // please create "topic1" on kafka and produce some data to it

    KafkaStorageFactory offsetStorageFactory = new KafkaStorageFactory("localhost:2181", "localhost:9092");

    KafkaSource source = new KafkaSource("topic1", "localhost:2181", offsetStorageFactory);
    Processor sourceProcessor = Processor.source(source, sourceNum, "kafka_source", appConfig, context.system());

    // For split task, we config to create two tasks
    int splitTaskNumber = 2;
    Processor split = new Processor(Split.class, splitTaskNumber);

    // sum task
    int sumTaskNumber = 2;
    Processor sum = new Processor(Sum.class, sumTaskNumber);

    // hbase sink
    int sinkNumber = 2;
    // please create HBase table "pipeline" and column family "wordcount"
    UserConfig config =
        UserConfig.empty().withString(HBaseSinkTask.ZOOKEEPER_QUORUM, "localhost:2181")
            .withString(HBaseSinkTask.TABLE_NAME, "pipeline")
            .withString(HBaseSinkTask.COLUMN_FAMILY, "wordcount");

    Processor sinkProcessor = new Processor(HBaseSinkTask.class, sinkNumber, "hbase_sink", config);


    Partitioner shuffle = new ShufflePartitioner();
    Partitioner hash = new HashPartitioner();

    Graph graph = new Graph();
    graph.addVertex(sourceProcessor);
    graph.addVertex(split);
    graph.addVertex(sum);
    graph.addVertex(sinkProcessor);

    graph.addEdge(sourceProcessor, shuffle, split);
    graph.addEdge(split, hash, sum);
    graph.addEdge(sum, hash, sinkProcessor);

    StreamApplication app = new StreamApplication("KafkaWordCount", appConfig, graph);

    context.submit(app);

    // clean resource
    context.close();
  }
}

