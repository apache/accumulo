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

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.util.EnumSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BadIteratorMincIT extends AccumuloClusterIT {

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();

    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    IteratorSetting is = new IteratorSetting(30, BadIterator.class);
    c.tableOperations().attachIterator(tableName, is, EnumSet.of(IteratorScope.minc));
    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());

    Mutation m = new Mutation(new Text("r1"));
    m.put(new Text("acf"), new Text(tableName), new Value("1".getBytes(UTF_8)));

    bw.addMutation(m);
    bw.close();

    c.tableOperations().flush(tableName, null, null, false);
    UtilWaitThread.sleep(1000);

    // minc should fail, so there should be no files
    FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 0, 0);

    // try to scan table
    Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY);
    int count = FunctionalTestUtils.count(scanner);
    assertEquals("Did not see expected # entries " + count, 1, count);

    // remove the bad iterator
    c.tableOperations().removeIterator(tableName, BadIterator.class.getSimpleName(), EnumSet.of(IteratorScope.minc));

    UtilWaitThread.sleep(5000);

    // minc should complete
    FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 1, 1);

    count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }

    if (count != 1)
      throw new Exception("Did not see expected # entries " + count);

    // now try putting bad iterator back and deleting the table
    c.tableOperations().attachIterator(tableName, is, EnumSet.of(IteratorScope.minc));
    bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    m = new Mutation(new Text("r2"));
    m.put(new Text("acf"), new Text(tableName), new Value("1".getBytes(UTF_8)));
    bw.addMutation(m);
    bw.close();

    // make sure property is given time to propagate
    UtilWaitThread.sleep(500);

    c.tableOperations().flush(tableName, null, null, false);

    // make sure the flush has time to start
    UtilWaitThread.sleep(1000);

    // this should not hang
    c.tableOperations().delete(tableName);
  }

}
