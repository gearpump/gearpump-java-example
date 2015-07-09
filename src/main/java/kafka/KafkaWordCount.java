package kafka;


import javatemplate.*;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.partitioner.HashPartitioner;
import org.apache.gearpump.partitioner.Partitioner;
import org.apache.gearpump.streaming.Processor;
import org.apache.gearpump.streaming.Processor$;
import org.apache.gearpump.streaming.StreamApplication;
import org.apache.gearpump.streaming.kafka.KafkaSink;
import org.apache.gearpump.streaming.kafka.KafkaSource;
import org.apache.gearpump.streaming.sink.DataSinkProcessor;
import org.apache.gearpump.streaming.source.DataSourceProcessor;
import org.apache.gearpump.util.Graph;

public class KafkaWordCount {

    public static void main(String[] args) {

        ClientContext context = ClientContext.apply();

        UserConfig appConfig = UserConfig.empty();

        // we will create two kafka reader task to read from kafka queue.
        int sourceNum = 2;
        KafkaSource source = new KafkaSource("topic1", "localhost:2181");
        Processor sourceProcessor = DataSourceProcessor.apply(source, sourceNum, "kafkasource", appConfig, context.system());

        // For split task, we config to create two tasks
        int splitTaskNumber = 2;
        Processor split = Processor$.MODULE$.apply(Split.class, splitTaskNumber, "split", appConfig);

        // sum task
        int sumTaskNumber = 2;
        Processor sum = Processor$.MODULE$.apply(Sum.class, sumTaskNumber, "sum", appConfig);

        // kafka sink
        int sinkNumber = 2;
        KafkaSink sink = new KafkaSink("topic2", "localhost:9092");
        Processor sinkProcessor = DataSinkProcessor.apply(sink, sinkNumber, "sink", appConfig, context.system());

        Partitioner partitioner = new HashPartitioner();

        Graph graph = Graph.empty();
        graph.addVertex(sourceProcessor);
        graph.addVertex(split);
        graph.addVertex(sum);
        graph.addVertex(sinkProcessor);

        graph.addEdge(sourceProcessor, partitioner, split);
        graph.addEdge(split, partitioner, sum);
        graph.addEdge(sum, partitioner, sinkProcessor);

        StreamApplication app = StreamApplication.apply("KafkaWordCount", graph, appConfig);

        context.submit(app);

        // clean resource
        context.close();
    }
}

