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

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
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
    c.instanceOperations().waitForBalance();
  }
}
