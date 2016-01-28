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

import io.gearpump.Message;
import io.gearpump.cluster.UserConfig;
import io.gearpump.external.hbase.HBaseSink;
import io.gearpump.streaming.task.StartTime;
import io.gearpump.streaming.task.Task;
import io.gearpump.streaming.task.TaskContext;
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


    sink = new HBaseSink(userConf, tableName, hbaseConf);
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
