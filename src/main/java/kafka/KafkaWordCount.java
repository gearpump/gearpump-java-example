package kafka;


import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.partitioner.HashPartitioner;
import org.apache.gearpump.partitioner.Partitioner;
import org.apache.gearpump.partitioner.ShufflePartitioner;
import org.apache.gearpump.streaming.Processor;
import org.apache.gearpump.streaming.Processor.DefaultProcessor;
import org.apache.gearpump.streaming.StreamApplication;
import org.apache.gearpump.streaming.kafka.KafkaSource;
import org.apache.gearpump.streaming.source.DataSourceProcessor;
import org.apache.gearpump.util.Graph;

public class KafkaWordCount {

    public static void main(String[] args) {

        ClientContext context = ClientContext.apply();

        UserConfig appConfig = UserConfig.empty();

        // we will create two kafka reader task to read from kafka queue.
        int sourceNum = 2;
        // please create "topic1" on kafka and produce some data to it
        KafkaSource source = new KafkaSource("topic1", "localhost:2181");
        Processor sourceProcessor = DataSourceProcessor.apply(source, sourceNum, "kafka_source", appConfig, context.system());

        // For split task, we config to create two tasks
        int splitTaskNumber = 2;
        Processor split = new DefaultProcessor(splitTaskNumber, "split", null, Split.class);

        // sum task
        int sumTaskNumber = 2;
        Processor sum = new DefaultProcessor(sumTaskNumber, "sum", null, Sum.class);

        // hbase sink
        int sinkNumber = 2;
        // please create HBase table "pipeline" and column family "wordcount"
        UserConfig config = UserConfig.empty()
                .withString(HBaseSinkTask.ZOOKEEPER_QUORUM, "localhost:2181")
                .withString(HBaseSinkTask.TABLE_NAME, "pipeline")
                .withString(HBaseSinkTask.COLUMN_FAMILY, "wordcount");
        Processor sinkProcessor = new DefaultProcessor(sinkNumber, "hbase_sink", config, HBaseSinkTask.class);


        Partitioner shuffle = new ShufflePartitioner();
        Partitioner hash = new HashPartitioner();

        Graph graph = Graph.empty();
        graph.addVertex(sourceProcessor);
        graph.addVertex(split);
        graph.addVertex(sum);
        graph.addVertex(sinkProcessor);

        graph.addEdge(sourceProcessor, shuffle, split);
        graph.addEdge(split, hash, sum);
        graph.addEdge(sum, hash, sinkProcessor);

        StreamApplication app = StreamApplication.apply("KafkaWordCount", graph, appConfig);

        context.submit(app);

        // clean resource
        context.close();
    }
}

