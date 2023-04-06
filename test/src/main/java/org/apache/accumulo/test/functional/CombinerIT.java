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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Collections;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.jupiter.api.Test;

public class CombinerIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  private void checkSum(String tableName, AccumuloClient c) throws Exception {
    try (Scanner s = c.createScanner(tableName, Authorizations.EMPTY)) {
      String actual = getOnlyElement(s).getValue().toString();
      assertEquals("45", actual);
    }
  }

  @Test
  public void aggregationTest() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      IteratorSetting setting = new IteratorSetting(10, SummingCombiner.class);
      SummingCombiner.setEncodingType(setting, Type.STRING);
      SummingCombiner.setColumns(setting,
          Collections.singletonList(new IteratorSetting.Column("cf")));
      c.tableOperations().attachIterator(tableName, setting);
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (int i = 0; i < 10; i++) {
          Mutation m = new Mutation("row1");
          m.put("cf".getBytes(), "col1".getBytes(), ("" + i).getBytes());
          bw.addMutation(m);
        }
      }
      checkSum(tableName, c);
    }
  }
}
