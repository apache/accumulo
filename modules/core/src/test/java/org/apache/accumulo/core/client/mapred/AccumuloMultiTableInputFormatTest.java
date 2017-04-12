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
package org.apache.accumulo.core.client.mapred;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.InputTableConfig;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class AccumuloMultiTableInputFormatTest {

  @Rule
  public TestName testName = new TestName();

  /**
   * Verify {@link org.apache.accumulo.core.client.mapreduce.InputTableConfig} objects get correctly serialized in the JobContext.
   */
  @Test
  public void testTableQueryConfigSerialization() throws IOException {
    String table1Name = testName.getMethodName() + "1";
    String table2Name = testName.getMethodName() + "2";
    JobConf job = new JobConf();

    InputTableConfig table1 = new InputTableConfig().setRanges(Collections.singletonList(new Range("a", "b")))
        .fetchColumns(Collections.singleton(new Pair<>(new Text("CF1"), new Text("CQ1"))))
        .setIterators(Collections.singletonList(new IteratorSetting(50, "iter1", "iterclass1")));

    InputTableConfig table2 = new InputTableConfig().setRanges(Collections.singletonList(new Range("a", "b")))
        .fetchColumns(Collections.singleton(new Pair<>(new Text("CF1"), new Text("CQ1"))))
        .setIterators(Collections.singletonList(new IteratorSetting(50, "iter1", "iterclass1")));

    Map<String,InputTableConfig> configMap = new HashMap<>();
    configMap.put(table1Name, table1);
    configMap.put(table2Name, table2);
    AccumuloMultiTableInputFormat.setInputTableConfigs(job, configMap);

    assertEquals(table1, AccumuloMultiTableInputFormat.getInputTableConfig(job, table1Name));
    assertEquals(table2, AccumuloMultiTableInputFormat.getInputTableConfig(job, table2Name));
  }
}
