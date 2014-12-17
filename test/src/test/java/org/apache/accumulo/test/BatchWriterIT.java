/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.junit.Test;

public class BatchWriterIT extends AccumuloClusterIT {

  @Override
  public int defaultTimeoutSeconds() {
    return 30;
  }

  @Test
  public void test() throws Exception {
    // call the batchwriter with buffer of size zero
    String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(table);
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxMemory(0);
    BatchWriter writer = c.createBatchWriter(table, config);
    Mutation m = new Mutation("row");
    m.put("cf", "cq", new Value("value".getBytes()));
    writer.addMutation(m);
    writer.close();
  }

}
