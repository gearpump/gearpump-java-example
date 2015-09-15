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

package kafka2kafka;

import io.gearpump.cluster.UserConfig;
import io.gearpump.cluster.client.ClientContext;
import io.gearpump.partitioner.HashPartitioner;
import io.gearpump.partitioner.Partitioner;
import io.gearpump.partitioner.ShufflePartitioner;
import io.gearpump.streaming.javaapi.Graph;
import io.gearpump.streaming.javaapi.Processor;
import io.gearpump.streaming.javaapi.StreamApplication;
import io.gearpump.streaming.kafka.KafkaSink;
import io.gearpump.streaming.kafka.KafkaSource;
import io.gearpump.streaming.kafka.KafkaStorageFactory;

public class PipelineApp {

  public static void main(String[] args) {
    ClientContext context = ClientContext.apply();
    UserConfig appConfig = UserConfig.empty();

    int taskNumber = 1;

    KafkaStorageFactory offsetStorageFactory = new KafkaStorageFactory("localhost:2181", "localhost:9092");

    // kafka source
    KafkaSource kafkaSource = new KafkaSource("inputTopic", "localhost:2181", offsetStorageFactory);
    Processor sourceProcessor = Processor.source(kafkaSource, taskNumber, "kafkaSource",
        appConfig, context.system());

    // converter (converts byte[] message to String -- kafka produces byte[])
    Processor convert2StringProcessor = new Processor(ByteArray2StringTask.class, taskNumber, "converter", null);

    // converter (converts String message to scala.Tuple2 -- kafka sink needs it)
    Processor convert2TupleProcessor = new Processor(String2Tuple2Task.class, taskNumber, "converter", null);

    // simple processor (represents processing you would do on kafka messages stream; writes payload to logs)
    Processor logProcessor = new Processor(LogMessageTask.class, taskNumber, "forwarder", null);

    // kafka sink
    KafkaSink kafkaSink = new KafkaSink("outputTopic", "localhost:9092");
    Processor sinkProcessor = Processor.sink(kafkaSink, taskNumber, "sink", appConfig, context.system());

    Graph graph = new Graph();
    graph.addVertex(sourceProcessor);
    graph.addVertex(convert2StringProcessor);
    graph.addVertex(logProcessor);
    graph.addVertex(convert2TupleProcessor);
    graph.addVertex(sinkProcessor);

    Partitioner partitioner = new HashPartitioner();
    Partitioner shufflePartitioner = new ShufflePartitioner();

    graph.addEdge(sourceProcessor, shufflePartitioner, convert2StringProcessor);
    graph.addEdge(convert2StringProcessor, partitioner, logProcessor);
    graph.addEdge(logProcessor, partitioner, convert2TupleProcessor);
    graph.addEdge(convert2TupleProcessor, partitioner, sinkProcessor);

    // submit app
    StreamApplication app = new StreamApplication("kafka2kafka", appConfig, graph);
    context.submit(app);

    // clean resource
    context.close();
  }
}
