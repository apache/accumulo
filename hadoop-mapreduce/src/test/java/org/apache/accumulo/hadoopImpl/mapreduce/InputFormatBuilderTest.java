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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/*
 This unit tests ClassLoaderContext and ExecuteHints functionality
 */
public class InputFormatBuilderTest {

  private class InputFormatBuilderImplTest<T> extends InputFormatBuilderImpl<T> {
    private String currentTable;
    private SortedMap<String,InputTableConfig> tableConfigMap = new TreeMap<>();
    private SortedMap<String,String> newHints = new TreeMap<>();

    private InputFormatBuilderImplTest(Class<T> callingClass) {
      super(callingClass);
    }

    @Override
    public InputFormatBuilder.InputFormatOptions<T> table(String tableName) {
      this.currentTable = tableName;
      tableConfigMap.put(currentTable, new InputTableConfig());
      return this;
    }

    @Override
    public InputFormatBuilder.InputFormatOptions<T> classLoaderContext(String context) {
      tableConfigMap.get(currentTable).setContext(context);
      return this;
    }

    private Optional<String> getClassLoaderContext() {
      return tableConfigMap.get(currentTable).getContext();
    }

    @Override
    public InputFormatOptions<T> consistencyLevel(ConsistencyLevel level) {
      tableConfigMap.get(currentTable).setConsistencyLevel(level);
      return this;
    }

    private ConsistencyLevel getConsistencyLevel() {
      return tableConfigMap.get(currentTable).getConsistencyLevel();
    }

    @Override
    public InputFormatBuilder.InputFormatOptions<T> executionHints(Map<String,String> hints) {
      this.newHints.putAll(hints);
      tableConfigMap.get(currentTable).setExecutionHints(hints);
      return this;
    }

    private SortedMap<String,String> getExecutionHints() {
      return newHints;
    }
  }

  private InputTableConfig tableQueryConfig;
  private InputFormatBuilderImplTest<InputFormatBuilderTest> formatBuilderTest;

  @BeforeEach
  public void setUp() {
    tableQueryConfig = new InputTableConfig();
    formatBuilderTest = new InputFormatBuilderImplTest<>(InputFormatBuilderTest.class);
    formatBuilderTest.table("test");
  }

  @Test
  public void testInputFormatBuilder_ClassLoaderContext() {
    String context = "classLoaderContext";

    InputFormatBuilderImpl<JobConf> formatBuilder =
        new InputFormatBuilderImpl<>(InputFormatBuilderTest.class);
    formatBuilder.table("test");
    formatBuilder.classLoaderContext(context);

    Optional<String> classLoaderContextStr = tableQueryConfig.getContext();
    assertTrue(classLoaderContextStr.toString().contains("empty")); // returns Optional.empty
  }

  @Test
  public void testInputFormatBuilderImplTest_ClassLoaderContext() {
    String context = "classLoaderContext";

    formatBuilderTest.classLoaderContext(context);

    Optional<String> classLoaderContextStr = formatBuilderTest.getClassLoaderContext();
    assertEquals(context, classLoaderContextStr.get());
  }

  @Test
  public void testInputFormatBuilderImplTest_consistencyLevel() {
    formatBuilderTest.consistencyLevel(ConsistencyLevel.EVENTUAL);
    ConsistencyLevel level = formatBuilderTest.getConsistencyLevel();
    assertEquals(ConsistencyLevel.EVENTUAL, level);
  }

  @Test
  public void testInputFormatBuilderImplTest_ExecuteHints() {
    SortedMap<String,String> hints = new TreeMap<>();
    hints.put("key1", "value1");
    hints.put("key2", "value2");
    hints.put("key3", "value3");

    formatBuilderTest.executionHints(hints);

    SortedMap<String,String> executionHints = formatBuilderTest.getExecutionHints();
    assertEquals(hints.toString(), executionHints.toString());
  }
}
