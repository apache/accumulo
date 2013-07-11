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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class MergeMetaIT extends MacTest {
  
  @Test(timeout = 60 * 1000)
  public void mergeMeta() throws Exception {
    Connector c = getConnector();
    SortedSet<Text> splits = new TreeSet<Text>();
    for (String id : "1 2 3 4 5".split(" ")) {
      splits.add(new Text(id));
    }
    c.tableOperations().addSplits(MetadataTable.NAME, splits);
    for (String tableName : "a1 a2 a3 a4 a5".split(" ")) {
      c.tableOperations().create(tableName);
    }
    c.tableOperations().merge(MetadataTable.NAME, null, null);
    UtilWaitThread.sleep(2 * 1000);
    Scanner s = c.createScanner(RootTable.NAME, Authorizations.EMPTY);
    s.setRange(MetadataSchema.DeletesSection.getRange());
    int count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> e : s) {
      count++;
    }
    assertTrue(count > 0);
    assertEquals(0, c.tableOperations().listSplits(MetadataTable.NAME).size());
  }
  
}
