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
package org.apache.accumulo.hadoopImpl.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InputTableConfigTest {

  private InputTableConfig tableQueryConfig;

  @BeforeEach
  public void setUp() {
    tableQueryConfig = new InputTableConfig();
  }

  @Test
  public void testSerialization_OnlyTable() throws IOException {
    byte[] serialized = serialize(tableQueryConfig);
    InputTableConfig actualConfig = deserialize(serialized);

    assertEquals(tableQueryConfig, actualConfig);
  }

  @Test
  public void testSerialization_consistencyLevel() throws IOException {
    byte[] serialized = serialize(tableQueryConfig);
    InputTableConfig actualConfig = deserialize(serialized);
    assertNull(actualConfig.getConsistencyLevel());
    assertEquals(tableQueryConfig, actualConfig);

    tableQueryConfig.setConsistencyLevel(ConsistencyLevel.IMMEDIATE);
    serialized = serialize(tableQueryConfig);
    actualConfig = deserialize(serialized);
    assertEquals(ConsistencyLevel.IMMEDIATE, actualConfig.getConsistencyLevel());
    assertEquals(tableQueryConfig, actualConfig);

    tableQueryConfig.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
    serialized = serialize(tableQueryConfig);
    actualConfig = deserialize(serialized);
    assertEquals(ConsistencyLevel.EVENTUAL, actualConfig.getConsistencyLevel());
    assertEquals(tableQueryConfig, actualConfig);
  }

  @Test
  public void testSerialization_allBooleans() throws IOException {
    tableQueryConfig.setAutoAdjustRanges(false);
    tableQueryConfig.setOfflineScan(true);
    tableQueryConfig.setUseIsolatedScanners(true);
    tableQueryConfig.setUseLocalIterators(true);
    byte[] serialized = serialize(tableQueryConfig);
    InputTableConfig actualConfig = deserialize(serialized);

    assertEquals(tableQueryConfig, actualConfig);
  }

  @Test
  public void testSerialization_ranges() throws IOException {
    List<Range> ranges = new ArrayList<>();
    ranges.add(new Range("a", "b"));
    ranges.add(new Range("c", "d"));
    tableQueryConfig.setRanges(ranges);

    byte[] serialized = serialize(tableQueryConfig);
    InputTableConfig actualConfig = deserialize(serialized);

    assertEquals(ranges, actualConfig.getRanges());
  }

  @Test
  public void testSerialization_columns() throws IOException {
    Set<IteratorSetting.Column> columns = new HashSet<>();
    columns.add(new IteratorSetting.Column(new Text("cf1"), new Text("cq1")));
    columns.add(new IteratorSetting.Column(new Text("cf2"), null));
    tableQueryConfig.fetchColumns(columns);

    byte[] serialized = serialize(tableQueryConfig);
    InputTableConfig actualConfig = deserialize(serialized);

    assertEquals(actualConfig.getFetchedColumns(), columns);
  }

  @Test
  public void testSerialization_iterators() throws IOException {
    List<IteratorSetting> settings = new ArrayList<>();
    settings.add(new IteratorSetting(50, "iter", "iterclass"));
    settings.add(new IteratorSetting(55, "iter2", "iterclass2"));
    settings.forEach(itr -> tableQueryConfig.addIterator(itr));
    byte[] serialized = serialize(tableQueryConfig);
    InputTableConfig actualConfig = deserialize(serialized);
    assertEquals(actualConfig.getIterators(), settings);

  }

  @Test
  public void testSamplerConfig() throws IOException {
    SamplerConfiguration sc = new SamplerConfiguration("com.foo.S1").addOption("k1", "v1");
    tableQueryConfig.setSamplerConfiguration(sc);
    InputTableConfig actualConfig = deserialize(serialize(tableQueryConfig));
    assertEquals(sc, actualConfig.getSamplerConfiguration());
  }

  @Test
  public void testExecutionHints() throws IOException {
    tableQueryConfig.setExecutionHints(Map.of("priority", "9"));
    InputTableConfig actualConfig = deserialize(serialize(tableQueryConfig));
    assertEquals(Map.of("priority", "9"), actualConfig.getExecutionHints());
  }

  private byte[] serialize(InputTableConfig tableQueryConfig) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    tableQueryConfig.write(new DataOutputStream(baos));
    baos.close();
    return baos.toByteArray();
  }

  private InputTableConfig deserialize(byte[] bytes) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    InputTableConfig actualConfig = new InputTableConfig(new DataInputStream(bais));
    bais.close();
    return actualConfig;
  }
}
