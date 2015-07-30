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

package org.apache.accumulo.test.examples.simple.filedata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.examples.simple.filedata.ChunkInputStream;
import org.apache.accumulo.examples.simple.filedata.FileDataIngest;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class ChunkInputStreamIT extends AccumuloClusterHarness {

  private static final Authorizations AUTHS = new Authorizations("A", "B", "C", "D");

  private Connector conn;
  private String tableName;
  private List<Entry<Key,Value>> data;
  private List<Entry<Key,Value>> baddata;
  private List<Entry<Key,Value>> multidata;

  @Before
  public void setupInstance() throws Exception {
    conn = getConnector();
    tableName = getUniqueNames(1)[0];
    conn.securityOperations().changeUserAuthorizations(conn.whoami(), AUTHS);
  }

  @Before
  public void setupData() {
    data = new ArrayList<Entry<Key,Value>>();
    addData(data, "a", "refs", "id\0ext", "A&B", "ext");
    addData(data, "a", "refs", "id\0name", "A&B", "name");
    addData(data, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
    addData(data, "a", "~chunk", 100, 1, "A&B", "");
    addData(data, "b", "refs", "id\0ext", "A&B", "ext");
    addData(data, "b", "refs", "id\0name", "A&B", "name");
    addData(data, "b", "~chunk", 100, 0, "A&B", "qwertyuiop");
    addData(data, "b", "~chunk", 100, 0, "B&C", "qwertyuiop");
    addData(data, "b", "~chunk", 100, 1, "A&B", "");
    addData(data, "b", "~chunk", 100, 1, "B&C", "");
    addData(data, "b", "~chunk", 100, 1, "D", "");
    addData(data, "c", "~chunk", 100, 0, "A&B", "asdfjkl;");
    addData(data, "c", "~chunk", 100, 1, "A&B", "asdfjkl;");
    addData(data, "c", "~chunk", 100, 2, "A&B", "");
    addData(data, "d", "~chunk", 100, 0, "A&B", "");
    addData(data, "e", "~chunk", 100, 0, "A&B", "asdfjkl;");
    addData(data, "e", "~chunk", 100, 1, "A&B", "");
    baddata = new ArrayList<Entry<Key,Value>>();
    addData(baddata, "a", "~chunk", 100, 0, "A", "asdfjkl;");
    addData(baddata, "b", "~chunk", 100, 0, "B", "asdfjkl;");
    addData(baddata, "b", "~chunk", 100, 2, "C", "");
    addData(baddata, "c", "~chunk", 100, 0, "D", "asdfjkl;");
    addData(baddata, "c", "~chunk", 100, 2, "E", "");
    addData(baddata, "d", "~chunk", 100, 0, "F", "asdfjkl;");
    addData(baddata, "d", "~chunk", 100, 1, "G", "");
    addData(baddata, "d", "~zzzzz", "colq", "H", "");
    addData(baddata, "e", "~chunk", 100, 0, "I", "asdfjkl;");
    addData(baddata, "e", "~chunk", 100, 1, "J", "");
    addData(baddata, "e", "~chunk", 100, 2, "I", "asdfjkl;");
    addData(baddata, "f", "~chunk", 100, 2, "K", "asdfjkl;");
    addData(baddata, "g", "~chunk", 100, 0, "L", "");
    multidata = new ArrayList<Entry<Key,Value>>();
    addData(multidata, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
    addData(multidata, "a", "~chunk", 100, 1, "A&B", "");
    addData(multidata, "a", "~chunk", 200, 0, "B&C", "asdfjkl;");
    addData(multidata, "b", "~chunk", 100, 0, "A&B", "asdfjkl;");
    addData(multidata, "b", "~chunk", 200, 0, "B&C", "asdfjkl;");
    addData(multidata, "b", "~chunk", 200, 1, "B&C", "asdfjkl;");
    addData(multidata, "c", "~chunk", 100, 0, "A&B", "asdfjkl;");
    addData(multidata, "c", "~chunk", 100, 1, "B&C", "");
  }

  static void addData(List<Entry<Key,Value>> data, String row, String cf, String cq, String vis, String value) {
    data.add(new KeyValue(new Key(new Text(row), new Text(cf), new Text(cq), new Text(vis)), value.getBytes()));
  }

  static void addData(List<Entry<Key,Value>> data, String row, String cf, int chunkSize, int chunkCount, String vis, String value) {
    Text chunkCQ = new Text(FileDataIngest.intToBytes(chunkSize));
    chunkCQ.append(FileDataIngest.intToBytes(chunkCount), 0, 4);
    data.add(new KeyValue(new Key(new Text(row), new Text(cf), chunkCQ, new Text(vis)), value.getBytes()));
  }

  @Test
  public void testWithAccumulo() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException {
    conn.tableOperations().create(tableName);
    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

    for (Entry<Key,Value> e : data) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), e.getValue());
      bw.addMutation(m);
    }
    bw.close();

    Scanner scan = conn.createScanner(tableName, AUTHS);

    ChunkInputStream cis = new ChunkInputStream();
    byte[] b = new byte[20];
    int read;
    PeekingIterator<Entry<Key,Value>> pi = new PeekingIterator<Entry<Key,Value>>(scan.iterator());

    cis.setSource(pi);
    assertEquals(read = cis.read(b), 8);
    assertEquals(new String(b, 0, read), "asdfjkl;");
    assertEquals(read = cis.read(b), -1);

    cis.setSource(pi);
    assertEquals(read = cis.read(b), 10);
    assertEquals(new String(b, 0, read), "qwertyuiop");
    assertEquals(read = cis.read(b), -1);
    assertEquals(cis.getVisibilities().toString(), "[A&B, B&C, D]");
    cis.close();

    cis.setSource(pi);
    assertEquals(read = cis.read(b), 16);
    assertEquals(new String(b, 0, read), "asdfjkl;asdfjkl;");
    assertEquals(read = cis.read(b), -1);
    assertEquals(cis.getVisibilities().toString(), "[A&B]");
    cis.close();

    cis.setSource(pi);
    assertEquals(read = cis.read(b), -1);
    cis.close();

    cis.setSource(pi);
    assertEquals(read = cis.read(b), 8);
    assertEquals(new String(b, 0, read), "asdfjkl;");
    assertEquals(read = cis.read(b), -1);
    cis.close();

    assertFalse(pi.hasNext());
  }

}
