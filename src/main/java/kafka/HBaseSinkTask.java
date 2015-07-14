package kafka;

import org.apache.gearpump.Message;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.external.hbase.HBaseSink;
import org.apache.gearpump.streaming.task.StartTime;
import org.apache.gearpump.streaming.task.Task;
import org.apache.gearpump.streaming.task.TaskContext;
import org.apache.hadoop.conf.Configuration;

public class HBaseSinkTask extends Task {

    public static String TABLE_NAME = "hbase.table.name";
    public static String COLUMN_FAMILY = "hbase.table.column.family";
    public static String COLUMN_NAME = "hbase.table.column.name";
    public static String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";


    private HBaseSink sink;
    private String columnFamily;

    public HBaseSinkTask(TaskContext taskContext, UserConfig userConf) {
        super(taskContext, userConf);

        String tableName = userConf.getString(TABLE_NAME).get();
        String zkQuorum = userConf.getString(ZOOKEEPER_QUORUM).get();

        columnFamily = userConf.getString(COLUMN_FAMILY).get();

        Configuration hbaseConf = new Configuration();
        hbaseConf.set(ZOOKEEPER_QUORUM, zkQuorum);


        sink = new HBaseSink(tableName, hbaseConf);
    }

    @Override
    public void onStart(StartTime startTime) {
        //skip
    }

    @Override
    public void onNext(Message message) {
        String[] wordcount = ((String) message.msg()).split(":");
        String word = wordcount[0];
        String count = wordcount[1];
        sink.insert(message.timestamp() + "", columnFamily, word, count);
    }
}
