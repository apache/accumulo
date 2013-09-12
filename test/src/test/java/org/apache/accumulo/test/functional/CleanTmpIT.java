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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.junit.Test;

public class CleanTmpIT extends SimpleMacIT {
  
  @Test(timeout = 2 * 60 * 1000)
  public void test() throws Exception {
    Connector c = getConnector();
    // make a table
    String tableName = makeTableName();
    c.tableOperations().create(tableName);
    // write to it
    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("row");
    m.put("cf", "cq", "value");
    bw.addMutation(m);
    bw.close();
    // take it offline
    c.tableOperations().offline(tableName);
    
    // create a fake _tmp file in its directory
    String id = c.tableOperations().tableIdMap().get(tableName);
    File tmp = File.createTempFile("junk", "_tmp", new File(getCluster().getConfig().getDir(), "accumulo/tables/" + id + "/default_tablet"));
    assertTrue(tmp.exists());
    c.tableOperations().online(tableName);
    UtilWaitThread.sleep(1000);
    assertFalse(tmp.exists());
  }
}
