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

import org.apache.gearpump.Message;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.streaming.task.StartTime;
import org.apache.gearpump.streaming.task.Task;
import org.apache.gearpump.streaming.task.TaskContext;
import org.slf4j.Logger;
import scala.Tuple2;

import java.io.UnsupportedEncodingException;

public class String2Tuple2Task extends Task {

    private TaskContext context;
    private UserConfig userConf;

    private Logger LOG = super.LOG();

    public String2Tuple2Task(TaskContext taskContext, UserConfig userConf) {
        super(taskContext, userConf);
        this.context = taskContext;
        this.userConf = userConf;
    }

    private Long now() {
        return System.currentTimeMillis();
    }

    @Override public void onStart(StartTime startTime) {
        LOG.info("String2Tuple2Task.onStart [" + startTime + "]");
    }

    @Override public void onNext(Message messagePayLoad) {
        LOG.info("String2Tuple2Task.onNext messagePayLoad = [" + messagePayLoad + "]");

        Object msg = messagePayLoad.msg();

        byte[] key = null;
        byte[] value = null;
        try {
            LOG.info("converting to Tuple2");
            key = "message".getBytes("UTF-8");
            value = ((String) msg).getBytes("UTF-8");
            Tuple2<byte[], byte[]> tuple = new Tuple2<byte[], byte[]>(key, value);
            context.output(new Message(tuple, now()));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            LOG.info("sending message as is.");
            context.output(new Message(msg, now()));
        }
    }
}
