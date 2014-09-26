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

import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

// Accumulo3047
public class BadDeleteMarkersCreatedIT extends ConfigurableMacIT {

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.GC_CYCLE_START, "0s");
  }

  private int timeoutFactor = 1;

  @Before
  public void getTimeoutFactor() {
    try {
      timeoutFactor = Integer.parseInt(System.getProperty("timeout.factor"));
    } catch (NumberFormatException e) {
      log.warn("Could not parse integer from timeout.factor");
    }

    Assert.assertTrue("timeout.factor must be greater than or equal to 1", timeoutFactor >= 1);
  }

  @Test
  public void test() throws Exception {
    // make a table
    String tableName = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(tableName);
    // add some splits
    SortedSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < 10; i++) {
      splits.add(new Text("" + i));
    }
    c.tableOperations().addSplits(tableName, splits);
    // get rid of all the splits
    c.tableOperations().deleteRows(tableName, null, null);
    // get rid of the table
    c.tableOperations().delete(tableName);
    // let gc run
    UtilWaitThread.sleep(timeoutFactor * 5 * 1000);
    // look for delete markers
    Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(MetadataSchema.DeletesSection.getRange());
    for (Entry<Key,Value> entry : scanner) {
      Assert.fail(entry.getKey().getRow().toString());
    }
  }

}
