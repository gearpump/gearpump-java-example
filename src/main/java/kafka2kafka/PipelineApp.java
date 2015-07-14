/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package kafka2kafka;

import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.partitioner.HashPartitioner;
import org.apache.gearpump.partitioner.Partitioner;
import org.apache.gearpump.partitioner.ShufflePartitioner;
import org.apache.gearpump.streaming.Processor;
import org.apache.gearpump.streaming.Processor.DefaultProcessor;
import org.apache.gearpump.streaming.StreamApplication;
import org.apache.gearpump.streaming.kafka.KafkaSink;
import org.apache.gearpump.streaming.kafka.KafkaSource;
import org.apache.gearpump.streaming.sink.DataSinkProcessor;
import org.apache.gearpump.streaming.source.DataSourceProcessor;
import org.apache.gearpump.util.Graph;

public class PipelineApp {

    public static void main(String[] args) {
        ClientContext context = ClientContext.apply();
        UserConfig appConfig = UserConfig.empty();

        int taskNumber = 1;

        // kafka source
        KafkaSource kafkaSource = new KafkaSource("inputTopic", "localhost:2181");
        Processor sourceProcessor = DataSourceProcessor.apply(kafkaSource, taskNumber, "kafkaSource", appConfig, context.system());

        // converter (converts byte[] message to String -- kafka produces byte[]
        Processor convert2BytesProcessor = new DefaultProcessor(taskNumber, "converter", null, ByteArray2StringTask.class);

        // converter (converts String message to scala.Tuple2 -- kafka sink needs it)
        Processor convertr2TupleProcessor = new DefaultProcessor(taskNumber, "converter", null, String2Tuple2Task.class);

        // simple processor (represents processing you would do on kafka messages stream; writes payload to logs)
        Processor logProcessor = new DefaultProcessor(taskNumber, "forwarder", null, LogMessageTask.class);

        // kafka sink
        KafkaSink kafkaSink = new KafkaSink("outputTopic", "localhost:9092");
        Processor sinkProcessor = DataSinkProcessor.apply(kafkaSink, taskNumber, "sink", appConfig, context.system());

        Graph graph = Graph.empty();
        graph.addVertex(sourceProcessor);
        graph.addVertex(convert2BytesProcessor);
        graph.addVertex(logProcessor);
        graph.addVertex(convertr2TupleProcessor);
        graph.addVertex(sinkProcessor);

        Partitioner partitioner = new HashPartitioner();
        Partitioner shufflePartitioner = new ShufflePartitioner();

        graph.addEdge(sourceProcessor, shufflePartitioner, convert2BytesProcessor);
        graph.addEdge(convert2BytesProcessor, partitioner, logProcessor);
        graph.addEdge(logProcessor, partitioner, convertr2TupleProcessor);

        graph.addEdge(convertr2TupleProcessor, partitioner, sinkProcessor);

        // submit app
        StreamApplication app = StreamApplication.apply("kafka2kafka", graph, appConfig);
        context.submit(app);

        // clean resource
        context.close();
    }
}
