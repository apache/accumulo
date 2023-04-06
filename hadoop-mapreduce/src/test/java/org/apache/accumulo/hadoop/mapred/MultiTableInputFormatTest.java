/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.hadoop.mapred;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.hadoop.its.WithTestNames;
import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder;
import org.apache.accumulo.hadoopImpl.mapreduce.InputTableConfig;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.InputConfigurator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.Test;

public class MultiTableInputFormatTest extends WithTestNames {
  public static final Class<AccumuloInputFormat> CLASS = AccumuloInputFormat.class;

  /**
   * Verify {@link InputTableConfig} objects get correctly serialized in the JobContext.
   */
  @Test
  public void testStoreTables() throws Exception {
    String table1Name = testName() + "1";
    String table2Name = testName() + "2";

    JobConf job = new JobConf();
    Properties clientProps =
        org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormatTest.setupClientProperties();
    List<Range> ranges = singletonList(new Range("a", "b"));
    Set<IteratorSetting.Column> cols =
        singleton(new IteratorSetting.Column(new Text("CF1"), new Text("CQ1")));
    IteratorSetting iter1 = new IteratorSetting(50, "iter1", "iterclass1");
    IteratorSetting iter2 = new IteratorSetting(60, "iter2", "iterclass2");
    List<IteratorSetting> allIters = new ArrayList<>();
    allIters.add(iter1);
    allIters.add(iter2);

    // if auths are not set client will try to get from server, we dont want that here
    Authorizations auths = Authorizations.EMPTY;

    // @formatter:off
    AccumuloInputFormat.configure().clientProperties(clientProps)
        .table(table1Name).auths(auths).ranges(ranges).fetchColumns(cols).addIterator(iter1)
        .addIterator(iter2).localIterators(true).offlineScan(true) // end table 1
        .table(table2Name).auths(auths).ranges(ranges).fetchColumns(cols).addIterator(iter2) // end
        .store(job);
    // @formatter:on

    InputTableConfig table1 = new InputTableConfig();
    table1.setScanAuths(auths).setRanges(ranges).fetchColumns(cols).setUseLocalIterators(true)
        .setOfflineScan(true);
    allIters.forEach(table1::addIterator);
    InputTableConfig table2 = new InputTableConfig();
    table2.setScanAuths(auths).setRanges(ranges).fetchColumns(cols).addIterator(iter2);

    assertEquals(table1, InputConfigurator.getInputTableConfig(CLASS, job, table1Name));
    assertEquals(table2, InputConfigurator.getInputTableConfig(CLASS, job, table2Name));
  }

  @Test
  public void testManyTables() throws Exception {
    JobConf job = new JobConf();
    Properties clientProps =
        org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormatTest.setupClientProperties();

    // if auths are not set client will try to get from server, we dont want that here
    Authorizations auths = Authorizations.EMPTY;

    // set the client properties once then loop over tables
    InputFormatBuilder.TableParams<JobConf> opts =
        AccumuloInputFormat.configure().clientProperties(clientProps);
    for (int i = 0; i < 10_000; i++) {
      List<Range> ranges = singletonList(new Range("a" + i, "b" + i));
      Set<Column> cols = singleton(new Column(new Text("CF" + i), new Text("CQ" + i)));
      IteratorSetting iter = new IteratorSetting(50, "iter" + i, "iterclass" + i);
      opts.table("table" + i).auths(auths).ranges(ranges).fetchColumns(cols).addIterator(iter);
    }
    opts.store(job);

    // verify
    Map<String,InputTableConfig> configs = InputConfigurator.getInputTableConfigs(CLASS, job);
    assertEquals(10_000, configs.size());

    // create objects to test against
    for (int i = 0; i < 10_000; i++) {
      InputTableConfig t = new InputTableConfig();
      List<Range> ranges = singletonList(new Range("a" + i, "b" + i));
      Set<Column> cols = singleton(new Column(new Text("CF" + i), new Text("CQ" + i)));
      IteratorSetting iter = new IteratorSetting(50, "iter" + i, "iterclass" + i);
      t.setScanAuths(auths).setRanges(ranges).fetchColumns(cols).addIterator(iter);
      assertEquals(t, configs.get("table" + i));
    }
  }
}
