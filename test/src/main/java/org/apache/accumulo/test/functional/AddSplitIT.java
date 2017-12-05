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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class AddSplitIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void addSplitTest() throws Exception {

    String tableName = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(tableName);

    insertData(tableName, 1l);

    TreeSet<Text> splits = new TreeSet<>();
    splits.add(new Text(String.format("%09d", 333)));
    splits.add(new Text(String.format("%09d", 666)));

    c.tableOperations().addSplits(tableName, splits);

    sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

    Collection<Text> actualSplits = c.tableOperations().listSplits(tableName);

    if (!splits.equals(new TreeSet<>(actualSplits))) {
      throw new Exception(splits + " != " + actualSplits);
    }

    verifyData(tableName, 1l);
    insertData(tableName, 2l);

    // did not clear splits on purpose, it should ignore existing split points
    // and still create the three additional split points

    splits.add(new Text(String.format("%09d", 200)));
    splits.add(new Text(String.format("%09d", 500)));
    splits.add(new Text(String.format("%09d", 800)));

    c.tableOperations().addSplits(tableName, splits);

    sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

    actualSplits = c.tableOperations().listSplits(tableName);

    if (!splits.equals(new TreeSet<>(actualSplits))) {
      throw new Exception(splits + " != " + actualSplits);
    }

    verifyData(tableName, 2l);
  }

  private void verifyData(String tableName, long ts) throws Exception {
    Scanner scanner = getConnector().createScanner(tableName, Authorizations.EMPTY);

    Iterator<Entry<Key,Value>> iter = scanner.iterator();

    for (int i = 0; i < 10000; i++) {
      if (!iter.hasNext()) {
        throw new Exception("row " + i + " not found");
      }

      Entry<Key,Value> entry = iter.next();

      String row = String.format("%09d", i);

      if (!entry.getKey().getRow().equals(new Text(row))) {
        throw new Exception("unexpected row " + entry.getKey() + " " + i);
      }

      if (entry.getKey().getTimestamp() != ts) {
        throw new Exception("unexpected ts " + entry.getKey() + " " + ts);
      }

      if (Integer.parseInt(entry.getValue().toString()) != i) {
        throw new Exception("unexpected value " + entry + " " + i);
      }
    }

    if (iter.hasNext()) {
      throw new Exception("found more than expected " + iter.next());
    }

  }

  private void insertData(String tableName, long ts) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException {
    BatchWriter bw = getConnector().createBatchWriter(tableName, null);

    for (int i = 0; i < 10000; i++) {
      String row = String.format("%09d", i);

      Mutation m = new Mutation(new Text(row));
      m.put(new Text("cf1"), new Text("cq1"), ts, new Value(Integer.toString(i).getBytes(UTF_8)));
      bw.addMutation(m);
    }

    bw.close();
  }

}
