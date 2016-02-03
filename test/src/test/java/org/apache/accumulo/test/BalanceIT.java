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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BalanceIT extends ConfigurableMacIT {

  @Test(timeout = 60 * 1000)
  public void testBalance() throws Exception {
    String tableName = getUniqueNames(1)[0];
    Connector c = getConnector();
    System.out.println("Creating table");
    c.tableOperations().create(tableName);
    SortedSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < 10; i++) {
      splits.add(new Text("" + i));
    }
    System.out.println("Adding splits");
    c.tableOperations().addSplits(tableName, splits);
    System.out.println("Waiting for balance");
    waitForBalance(c);
  }

  private void waitForBalance(Connector c) throws Exception {
    while (!isBalanced(c)) {
      UtilWaitThread.sleep(1000);
    }
  }

  private boolean isBalanced(Connector c) throws Exception {
    Map<String,Integer> counts = new HashMap<String,Integer>();
    Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(MetadataSchema.TabletsSection.getRange());
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
    for (Entry<Key,Value> entry : scanner) {
      String host = entry.getKey().getColumnQualifier().toString();
      Integer count = counts.get(host);
      if (count == null) {
        count = Integer.valueOf(0);
      }
      counts.put(host, count.intValue() + 1);
    }
    if (counts.size() < 2) {
      return false;
    }
    Iterator<Integer> iter = counts.values().iterator();
    int initial = iter.next().intValue();
    while (iter.hasNext()) {
      if (Math.abs(iter.next().intValue() - initial) > 2)
        return false;
    }
    return true;
  }

}
