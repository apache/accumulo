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
package org.apache.accumulo.hadoopImpl.mapreduce;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class InputFormatBuilderTest {

  private class InputFormatBuilderImplTest<T> extends InputFormatBuilderImpl<T> {
    private String currentTable;
    private Map<String,InputTableConfig> tableConfigMap = new LinkedHashMap<>();
    private Map<String,String> newHints;

    private InputFormatBuilderImplTest(Class callingClass) {
      super(callingClass);
    }

    public InputFormatBuilder.InputFormatOptions<T> table(String tableName) {
      this.currentTable = tableName;
      tableConfigMap.put(currentTable, new InputTableConfig());
      return this;
    }

    @Inject
    public InputFormatBuilder.InputFormatOptions<T> classLoaderContext(String context) {
      tableConfigMap.get(currentTable).setContext(context);
      return this;
    }

    private Optional<String> getClassLoaderContext() {
      return tableConfigMap.get(currentTable).getContext();
    }

    @Inject
    public InputFormatBuilder.InputFormatOptions<T> executionHints(Map<String,String> hints) {
      this.newHints = ImmutableMap.copyOf(hints);
      tableConfigMap.get(currentTable).setExecutionHints(hints);
      return this;
    }

    private Map<String,String> getExecutionHints() {
      return newHints;
    }
  }

  private InputTableConfig tableQueryConfig;
  private InputFormatBuilderImplTest formatBuilderTest;

  @Before
  public void setUp() {
    tableQueryConfig = new InputTableConfig();
    formatBuilderTest = new InputFormatBuilderImplTest(InputFormatBuilderTest.class);
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
    assertEquals(classLoaderContextStr.get(), context);
  }

  @Test
  public void testInputFormatBuilderImplTest_ExecuteHints() {
    Map<String,String> hints = ImmutableMap.<String,String>builder().put("key1", "value1")
        .put("key2", "value2").put("key3", "value3").build();

    formatBuilderTest.executionHints(hints);

    Map<String,String> executionHints = formatBuilderTest.getExecutionHints();
    assertEquals(executionHints.toString(), hints.toString());
  }
}
