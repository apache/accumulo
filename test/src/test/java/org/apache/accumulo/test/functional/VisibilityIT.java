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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ByteArraySet;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class VisibilityIT extends AccumuloClusterIT {

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void run() throws Exception {
    Connector c = getConnector();
    String[] tableNames = getUniqueNames(2);
    String table = tableNames[0];
    c.tableOperations().create(table);
    String table2 = tableNames[1];
    c.tableOperations().create(table2);
    c.tableOperations().setProperty(table2, Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey(), "DEFLABEL");

    insertData(c, table);
    queryData(c, table);
    deleteData(c, table);

    insertDefaultData(c, table2);
    queryDefaultData(c, table2);

  }

  private static SortedSet<String> nss(String... labels) {
    TreeSet<String> ts = new TreeSet<String>();

    for (String s : labels) {
      ts.add(s);
    }

    return ts;
  }

  private void mput(Mutation m, String cf, String cq, String cv, String val) {
    ColumnVisibility le = new ColumnVisibility(cv.getBytes(UTF_8));
    m.put(new Text(cf), new Text(cq), le, new Value(val.getBytes(UTF_8)));
  }

  private void mputDelete(Mutation m, String cf, String cq, String cv) {
    ColumnVisibility le = new ColumnVisibility(cv.getBytes(UTF_8));
    m.putDelete(new Text(cf), new Text(cq), le);
  }

  private void insertData(Connector c, String tableName) throws Exception {

    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m1 = new Mutation(new Text("row1"));

    mput(m1, "cf1", "cq1", "", "v1");
    mput(m1, "cf1", "cq1", "A", "v2");
    mput(m1, "cf1", "cq1", "B", "v3");
    mput(m1, "cf1", "cq1", "A&B", "v4");
    mput(m1, "cf1", "cq1", "A&(L|M)", "v5");
    mput(m1, "cf1", "cq1", "B&(L|M)", "v6");
    mput(m1, "cf1", "cq1", "A&B&(L|M)", "v7");
    mput(m1, "cf1", "cq1", "A&B&(L)", "v8");
    mput(m1, "cf1", "cq1", "A&FOO", "v9");
    mput(m1, "cf1", "cq1", "A&FOO&(L|M)", "v10");
    mput(m1, "cf1", "cq1", "FOO", "v11");
    mput(m1, "cf1", "cq1", "(A|B)&FOO&(L|M)", "v12");
    mput(m1, "cf1", "cq1", "A&B&(L|M|FOO)", "v13");

    bw.addMutation(m1);
    bw.close();
  }

  private void deleteData(Connector c, String tableName) throws Exception {

    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m1 = new Mutation(new Text("row1"));

    mputDelete(m1, "cf1", "cq1", "");
    mputDelete(m1, "cf1", "cq1", "A");
    mputDelete(m1, "cf1", "cq1", "A&B");
    mputDelete(m1, "cf1", "cq1", "B&(L|M)");
    mputDelete(m1, "cf1", "cq1", "A&B&(L)");
    mputDelete(m1, "cf1", "cq1", "A&FOO&(L|M)");
    mputDelete(m1, "cf1", "cq1", "(A|B)&FOO&(L|M)");
    mputDelete(m1, "cf1", "cq1", "FOO&A"); // should not delete anything

    bw.addMutation(m1);
    bw.close();

    Map<Set<String>,Set<String>> expected = new HashMap<Set<String>,Set<String>>();

    expected.put(nss("A", "L"), nss("v5"));
    expected.put(nss("A", "M"), nss("v5"));
    expected.put(nss("B"), nss("v3"));
    expected.put(nss("Z"), nss());
    expected.put(nss("A", "B", "L"), nss("v7", "v13"));
    expected.put(nss("A", "B", "M"), nss("v7", "v13"));
    expected.put(nss("A", "B", "FOO"), nss("v13"));
    expected.put(nss("FOO"), nss("v11"));
    expected.put(nss("A", "FOO"), nss("v9"));

    queryData(c, tableName, nss("A", "B", "FOO", "L", "M", "Z"), nss("A", "B", "FOO", "L", "M", "Z"), expected);
  }

  private void insertDefaultData(Connector c, String tableName) throws Exception {
    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m1 = new Mutation(new Text("row1"));

    mput(m1, "cf1", "cq1", "BASE", "v1");
    mput(m1, "cf1", "cq2", "DEFLABEL", "v2");
    mput(m1, "cf1", "cq3", "", "v3");

    bw.addMutation(m1);
    bw.close();
  }

  private static void uniqueCombos(List<Set<String>> all, Set<String> prefix, Set<String> suffix) {

    all.add(prefix);

    TreeSet<String> ss = new TreeSet<String>(suffix);

    for (String s : suffix) {
      TreeSet<String> ps = new TreeSet<String>(prefix);
      ps.add(s);
      ss.remove(s);

      uniqueCombos(all, ps, ss);
    }
  }

  private void queryData(Connector c, String tableName) throws Exception {
    Map<Set<String>,Set<String>> expected = new HashMap<Set<String>,Set<String>>();
    expected.put(nss(), nss("v1"));
    expected.put(nss("A"), nss("v2"));
    expected.put(nss("A", "L"), nss("v5"));
    expected.put(nss("A", "M"), nss("v5"));
    expected.put(nss("B"), nss("v3"));
    expected.put(nss("B", "L"), nss("v6"));
    expected.put(nss("B", "M"), nss("v6"));
    expected.put(nss("Z"), nss());
    expected.put(nss("A", "B"), nss("v4"));
    expected.put(nss("A", "B", "L"), nss("v7", "v8", "v13"));
    expected.put(nss("A", "B", "M"), nss("v7", "v13"));
    expected.put(nss("A", "B", "FOO"), nss("v13"));
    expected.put(nss("FOO"), nss("v11"));
    expected.put(nss("A", "FOO"), nss("v9"));
    expected.put(nss("A", "FOO", "L"), nss("v10", "v12"));
    expected.put(nss("A", "FOO", "M"), nss("v10", "v12"));
    expected.put(nss("B", "FOO", "L"), nss("v12"));
    expected.put(nss("B", "FOO", "M"), nss("v12"));

    queryData(c, tableName, nss("A", "B", "FOO", "L", "M", "Z"), nss("A", "B", "FOO", "L", "M", "Z"), expected);
    queryData(c, tableName, nss("A", "B", "FOO", "L", "M", "Z"), nss("A", "B", "L", "M", "Z"), expected);
    queryData(c, tableName, nss("A", "B", "FOO", "L", "M", "Z"), nss("A", "Z"), expected);
    queryData(c, tableName, nss("A", "B", "FOO", "L", "M", "Z"), nss("Z"), expected);
    queryData(c, tableName, nss("A", "B", "FOO", "L", "M", "Z"), nss(), expected);
  }

  private void queryData(Connector c, String tableName, Set<String> allAuths, Set<String> userAuths, Map<Set<String>,Set<String>> expected) throws Exception {

    c.securityOperations().changeUserAuthorizations("root", new Authorizations(nbas(userAuths)));

    ArrayList<Set<String>> combos = new ArrayList<Set<String>>();
    uniqueCombos(combos, nss(), allAuths);

    for (Set<String> set1 : combos) {
      Set<String> e = new TreeSet<String>();
      for (Set<String> set2 : combos) {

        set2 = new HashSet<String>(set2);
        set2.retainAll(userAuths);

        if (set1.containsAll(set2) && expected.containsKey(set2)) {
          e.addAll(expected.get(set2));
        }
      }

      set1.retainAll(userAuths);
      verify(c, tableName, set1, e);
    }

  }

  private void queryDefaultData(Connector c, String tableName) throws Exception {
    Scanner scanner;

    // should return no records
    c.securityOperations().changeUserAuthorizations("root", new Authorizations("BASE", "DEFLABEL"));
    scanner = getConnector().createScanner(tableName, new Authorizations());
    verifyDefault(scanner, 0);

    // should return one record
    scanner = getConnector().createScanner(tableName, new Authorizations("BASE"));
    verifyDefault(scanner, 1);

    // should return all three records
    scanner = getConnector().createScanner(tableName, new Authorizations("BASE", "DEFLABEL"));
    verifyDefault(scanner, 3);
  }

  private void verifyDefault(Scanner scanner, int expectedCount) throws Exception {
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner)
      --expectedCount;
    if (expectedCount != 0)
      throw new Exception(" expected count !=0 " + expectedCount);
  }

  private void verify(Connector c, String tableName, Set<String> auths, Set<String> expectedValues) throws Exception {
    ByteArraySet bas = nbas(auths);

    try {
      verify(c, tableName, bas, expectedValues.toArray(new String[0]));
    } catch (Exception e) {
      throw new Exception("Verification failed auths=" + auths + " exp=" + expectedValues, e);
    }
  }

  private ByteArraySet nbas(Set<String> auths) {
    ByteArraySet bas = new ByteArraySet();
    for (String auth : auths) {
      bas.add(auth.getBytes(UTF_8));
    }
    return bas;
  }

  private void verify(Connector c, String tableName, ByteArraySet nss, String... expected) throws Exception {
    Scanner scanner = c.createScanner(tableName, new Authorizations(nss));
    verify(scanner.iterator(), expected);

    BatchScanner bs = getConnector().createBatchScanner(tableName, new Authorizations(nss), 3);
    bs.setRanges(Collections.singleton(new Range()));
    verify(bs.iterator(), expected);
    bs.close();
  }

  private void verify(Iterator<Entry<Key,Value>> iter, String... expected) throws Exception {
    HashSet<String> valuesSeen = new HashSet<String>();

    while (iter.hasNext()) {
      Entry<Key,Value> entry = iter.next();
      if (valuesSeen.contains(entry.getValue().toString())) {
        throw new Exception("Value seen twice");
      }
      valuesSeen.add(entry.getValue().toString());
    }

    for (String ev : expected) {
      if (!valuesSeen.remove(ev)) {
        throw new Exception("Did not see expected value " + ev);
      }
    }

    if (valuesSeen.size() != 0) {
      throw new Exception("Saw more values than expected " + valuesSeen);
    }
  }
}
