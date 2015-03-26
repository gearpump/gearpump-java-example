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
 */

package javatemplate;

import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.partitioner.HashPartitioner;
import org.apache.gearpump.partitioner.Partitioner;
import org.apache.gearpump.streaming.AppDescription;
import org.apache.gearpump.streaming.TaskDescription;
import org.apache.gearpump.util.Graph;

public class WordCount {

  /**
   * example: WordCount 127.0.0.1:3000
   * @param args
   */
  public static void main(String[] args) {
    String master = args[0];

    ClientContext context = ClientContext.apply(master);

    //define the topology graph
    Graph<TaskDescription, Partitioner> graph = Graph.empty();

    // For split task, we config to create two tasks
    int splitTaskNumber = 2;
    TaskDescription split = new TaskDescription(Split.class.getName(), splitTaskNumber);

    // For sum task, we have two summer.
    int sumTaskNumber = 2;
    TaskDescription sum = new TaskDescription(Sum.class.getName(), sumTaskNumber);

    // construct the graph
    graph.addVertex(split);
    graph.addVertex(sum);
    Partitioner partitioner = new HashPartitioner();
    graph.addEdge(split, partitioner, sum);

    // submit
    AppDescription app = new AppDescription("javawordcount", UserConfig.empty(), graph, null);
    context.submit(AppDescription.AppDescriptionToApplication(app, context.system()));

    // clean resource
    context.close();
  }
}
