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
package org.apache.accumulo.core.client.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class AccumuloMultiTableInputFormatTest {

  @Rule
  public TestName testName = new TestName();

  /**
   * Verify {@link InputTableConfig} objects get correctly serialized in the JobContext.
   */
  @Test
  public void testInputTableConfigSerialization() throws IOException {
    String table1 = testName.getMethodName() + "1";
    String table2 = testName.getMethodName() + "2";
    Job job = Job.getInstance();

    InputTableConfig tableConfig = new InputTableConfig().setRanges(Collections.singletonList(new Range("a", "b")))
        .fetchColumns(Collections.singleton(new Pair<>(new Text("CF1"), new Text("CQ1"))))
        .setIterators(Collections.singletonList(new IteratorSetting(50, "iter1", "iterclass1")));

    Map<String,InputTableConfig> configMap = new HashMap<>();
    configMap.put(table1, tableConfig);
    configMap.put(table2, tableConfig);

    AccumuloMultiTableInputFormat.setInputTableConfigs(job, configMap);

    assertEquals(tableConfig, AccumuloMultiTableInputFormat.getInputTableConfig(job, table1));
    assertEquals(tableConfig, AccumuloMultiTableInputFormat.getInputTableConfig(job, table2));
  }

}
