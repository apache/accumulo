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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class WaitForBalanceIT extends ConfigurableMacBase {

  @Override
  public int defaultTimeoutSeconds() {
    return 120;
  }

  @Test
  public void test() throws Exception {
    final Connector c = getConnector();
    // ensure the metadata table is online
    Iterators.size(c.createScanner(MetadataTable.NAME, Authorizations.EMPTY).iterator());
    c.instanceOperations().waitForBalance();
    assertTrue(isBalanced());
    final String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.instanceOperations().waitForBalance();
    final SortedSet<Text> partitionKeys = new TreeSet<>();
    for (int i = 0; i < 1000; i++) {
      partitionKeys.add(new Text("" + i));
    }
    c.tableOperations().addSplits(tableName, partitionKeys);
    assertFalse(isBalanced());
    c.instanceOperations().waitForBalance();
    assertTrue(isBalanced());
  }

  private boolean isBalanced() throws Exception {
    final Map<String,Integer> counts = new HashMap<>();
    int offline = 0;
    final Connector c = getConnector();
    for (String tableName : new String[] {MetadataTable.NAME, RootTable.NAME}) {
      final Scanner s = c.createScanner(tableName, Authorizations.EMPTY);
      s.setRange(MetadataSchema.TabletsSection.getRange());
      s.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
      MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(s);
      String location = null;
      for (Entry<Key,Value> entry : s) {
        Key key = entry.getKey();
        if (key.getColumnFamily()
            .equals(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME)) {
          location = key.getColumnQualifier().toString();
        } else if (MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN
            .hasColumns(key)) {
          if (location == null) {
            offline++;
          } else {
            Integer count = counts.get(location);
            if (count == null)
              count = Integer.valueOf(0);
            count = Integer.valueOf(count.intValue() + 1);
            counts.put(location, count);
          }
          location = null;
        }
      }
    }
    // the replication table is expected to be offline for this test, so ignore it
    if (offline > 1) {
      System.out.println("Offline tablets " + offline);
      return false;
    }
    int average = 0;
    for (Integer i : counts.values()) {
      average += i;
    }
    average /= counts.size();
    System.out.println(counts);
    int tablesCount = c.tableOperations().list().size();
    for (Entry<String,Integer> hostCount : counts.entrySet()) {
      if (Math.abs(average - hostCount.getValue()) > tablesCount) {
        System.out.println(
            "Average " + average + " count " + hostCount.getKey() + ": " + hostCount.getValue());
        return false;
      }
    }
    return true;
  }

}
