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

import java.util.EnumSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// ACCUMULO-2880
public class DeletedTablesDontFlushIT extends SharedMiniClusterBase {

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    IteratorSetting setting = new IteratorSetting(100, SlowIterator.class);
    SlowIterator.setSleepTime(setting, 1000);
    c.tableOperations().attachIterator(tableName, setting, EnumSet.of(IteratorScope.minc));
    // let the configuration change propagate through zookeeper
    UtilWaitThread.sleep(1000);

    Mutation m = new Mutation("xyzzy");
    for (int i = 0; i < 100; i++) {
      m.put("cf", "" + i, new Value(new byte[] {}));
    }
    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    bw.addMutation(m);
    bw.close();
    // should go fast
    c.tableOperations().delete(tableName);
  }

}
