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
package org.apache.accumulo.examples.simple.filedata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class ChunkInputStreamTest extends TestCase {
  private static final Logger log = Logger.getLogger(ChunkInputStream.class);
  List<Entry<Key,Value>> data;
  List<Entry<Key,Value>> baddata;
  List<Entry<Key,Value>> multidata;

  {
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

  public static void addData(List<Entry<Key,Value>> data, String row, String cf, String cq, String vis, String value) {
    data.add(new KeyValue(new Key(new Text(row), new Text(cf), new Text(cq), new Text(vis)), value.getBytes()));
  }

  public static void addData(List<Entry<Key,Value>> data, String row, String cf, int chunkSize, int chunkCount, String vis, String value) {
    Text chunkCQ = new Text(FileDataIngest.intToBytes(chunkSize));
    chunkCQ.append(FileDataIngest.intToBytes(chunkCount), 0, 4);
    data.add(new KeyValue(new Key(new Text(row), new Text(cf), chunkCQ, new Text(vis)), value.getBytes()));
  }

  public void testExceptionOnMultipleSetSourceWithoutClose() throws IOException {
    ChunkInputStream cis = new ChunkInputStream();
    PeekingIterator<Entry<Key,Value>> pi = new PeekingIterator<Entry<Key,Value>>(data.iterator());
    pi = new PeekingIterator<Entry<Key,Value>>(data.iterator());
    cis.setSource(pi);
    try {
      cis.setSource(pi);
      assertNotNull(null);
    } catch (IOException e) {
      assertNull(null);
    }
    cis.close();
  }

  public void testExceptionOnGetVisBeforeClose() throws IOException {
    ChunkInputStream cis = new ChunkInputStream();
    PeekingIterator<Entry<Key,Value>> pi = new PeekingIterator<Entry<Key,Value>>(data.iterator());

    cis.setSource(pi);
    try {
      cis.getVisibilities();
      assertNotNull(null);
    } catch (RuntimeException e) {
      assertNull(null);
    }
    cis.close();
    cis.getVisibilities();
  }

  public void testReadIntoBufferSmallerThanChunks() throws IOException {
    ChunkInputStream cis = new ChunkInputStream();
    byte[] b = new byte[5];

    PeekingIterator<Entry<Key,Value>> pi = new PeekingIterator<Entry<Key,Value>>(data.iterator());

    cis.setSource(pi);
    int read;
    assertEquals(read = cis.read(b), 5);
    assertEquals(new String(b, 0, read), "asdfj");
    assertEquals(read = cis.read(b), 3);
    assertEquals(new String(b, 0, read), "kl;");
    assertEquals(read = cis.read(b), -1);

    cis.setSource(pi);
    assertEquals(read = cis.read(b), 5);
    assertEquals(new String(b, 0, read), "qwert");
    assertEquals(read = cis.read(b), 5);
    assertEquals(new String(b, 0, read), "yuiop");
    assertEquals(read = cis.read(b), -1);
    assertEquals(cis.getVisibilities().toString(), "[A&B, B&C, D]");
    cis.close();

    cis.setSource(pi);
    assertEquals(read = cis.read(b), 5);
    assertEquals(new String(b, 0, read), "asdfj");
    assertEquals(read = cis.read(b), 5);
    assertEquals(new String(b, 0, read), "kl;as");
    assertEquals(read = cis.read(b), 5);
    assertEquals(new String(b, 0, read), "dfjkl");
    assertEquals(read = cis.read(b), 1);
    assertEquals(new String(b, 0, read), ";");
    assertEquals(read = cis.read(b), -1);
    assertEquals(cis.getVisibilities().toString(), "[A&B]");
    cis.close();

    cis.setSource(pi);
    assertEquals(read = cis.read(b), -1);
    cis.close();

    cis.setSource(pi);
    assertEquals(read = cis.read(b), 5);
    assertEquals(new String(b, 0, read), "asdfj");
    assertEquals(read = cis.read(b), 3);
    assertEquals(new String(b, 0, read), "kl;");
    assertEquals(read = cis.read(b), -1);
    cis.close();

    assertFalse(pi.hasNext());
  }

  public void testReadIntoBufferLargerThanChunks() throws IOException {
    ChunkInputStream cis = new ChunkInputStream();
    byte[] b = new byte[20];
    int read;
    PeekingIterator<Entry<Key,Value>> pi = new PeekingIterator<Entry<Key,Value>>(data.iterator());

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

  public void testWithAccumulo() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException {
    Connector conn = new MockInstance().getConnector("root", new PasswordToken(""));
    conn.tableOperations().create("test");
    BatchWriter bw = conn.createBatchWriter("test", new BatchWriterConfig());

    for (Entry<Key,Value> e : data) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), e.getValue());
      bw.addMutation(m);
    }
    bw.close();

    Scanner scan = conn.createScanner("test", new Authorizations("A", "B", "C", "D"));

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

  private static void assumeExceptionOnRead(ChunkInputStream cis, byte[] b) {
    try {
      cis.read(b);
      assertNotNull(null);
    } catch (IOException e) {
      log.debug("EXCEPTION " + e.getMessage());
      assertNull(null);
    }
  }

  private static void assumeExceptionOnClose(ChunkInputStream cis) {
    try {
      cis.close();
      assertNotNull(null);
    } catch (IOException e) {
      log.debug("EXCEPTION " + e.getMessage());
      assertNull(null);
    }
  }

  public void testBadData() throws IOException {
    ChunkInputStream cis = new ChunkInputStream();
    byte[] b = new byte[20];
    int read;
    PeekingIterator<Entry<Key,Value>> pi = new PeekingIterator<Entry<Key,Value>>(baddata.iterator());

    cis.setSource(pi);
    assumeExceptionOnRead(cis, b);
    assumeExceptionOnClose(cis);
    // can still get visibilities after exception -- bad?
    assertEquals(cis.getVisibilities().toString(), "[A]");

    cis.setSource(pi);
    assumeExceptionOnRead(cis, b);
    assumeExceptionOnClose(cis);
    assertEquals(cis.getVisibilities().toString(), "[B, C]");

    cis.setSource(pi);
    assumeExceptionOnRead(cis, b);
    assumeExceptionOnClose(cis);
    assertEquals(cis.getVisibilities().toString(), "[D, E]");

    cis.setSource(pi);
    assertEquals(read = cis.read(b), 8);
    assertEquals(new String(b, 0, read), "asdfjkl;");
    assertEquals(read = cis.read(b), -1);
    assertEquals(cis.getVisibilities().toString(), "[F, G]");
    cis.close();

    cis.setSource(pi);
    assumeExceptionOnRead(cis, b);
    cis.close();
    assertEquals(cis.getVisibilities().toString(), "[I, J]");

    try {
      cis.setSource(pi);
      assertNotNull(null);
    } catch (IOException e) {
      assertNull(null);
    }
    assumeExceptionOnClose(cis);
    assertEquals(cis.getVisibilities().toString(), "[K]");

    cis.setSource(pi);
    assertEquals(read = cis.read(b), -1);
    assertEquals(cis.getVisibilities().toString(), "[L]");
    cis.close();

    assertFalse(pi.hasNext());

    pi = new PeekingIterator<Entry<Key,Value>>(baddata.iterator());
    cis.setSource(pi);
    assumeExceptionOnClose(cis);
  }

  public void testBadDataWithoutClosing() throws IOException {
    ChunkInputStream cis = new ChunkInputStream();
    byte[] b = new byte[20];
    int read;
    PeekingIterator<Entry<Key,Value>> pi = new PeekingIterator<Entry<Key,Value>>(baddata.iterator());

    cis.setSource(pi);
    assumeExceptionOnRead(cis, b);
    // can still get visibilities after exception -- bad?
    assertEquals(cis.getVisibilities().toString(), "[A]");

    cis.setSource(pi);
    assumeExceptionOnRead(cis, b);
    assertEquals(cis.getVisibilities().toString(), "[B, C]");

    cis.setSource(pi);
    assumeExceptionOnRead(cis, b);
    assertEquals(cis.getVisibilities().toString(), "[D, E]");

    cis.setSource(pi);
    assertEquals(read = cis.read(b), 8);
    assertEquals(new String(b, 0, read), "asdfjkl;");
    assertEquals(read = cis.read(b), -1);
    assertEquals(cis.getVisibilities().toString(), "[F, G]");
    cis.close();

    cis.setSource(pi);
    assumeExceptionOnRead(cis, b);
    assertEquals(cis.getVisibilities().toString(), "[I, J]");

    try {
      cis.setSource(pi);
      assertNotNull(null);
    } catch (IOException e) {
      assertNull(null);
    }
    assertEquals(cis.getVisibilities().toString(), "[K]");

    cis.setSource(pi);
    assertEquals(read = cis.read(b), -1);
    assertEquals(cis.getVisibilities().toString(), "[L]");
    cis.close();

    assertFalse(pi.hasNext());

    pi = new PeekingIterator<Entry<Key,Value>>(baddata.iterator());
    cis.setSource(pi);
    assumeExceptionOnClose(cis);
  }

  public void testMultipleChunkSizes() throws IOException {
    ChunkInputStream cis = new ChunkInputStream();
    byte[] b = new byte[20];
    int read;
    PeekingIterator<Entry<Key,Value>> pi = new PeekingIterator<Entry<Key,Value>>(multidata.iterator());

    b = new byte[20];

    cis.setSource(pi);
    assertEquals(read = cis.read(b), 8);
    assertEquals(read = cis.read(b), -1);
    cis.close();
    assertEquals(cis.getVisibilities().toString(), "[A&B]");

    cis.setSource(pi);
    assumeExceptionOnRead(cis, b);
    assertEquals(cis.getVisibilities().toString(), "[A&B]");

    cis.setSource(pi);
    assertEquals(read = cis.read(b), 8);
    assertEquals(new String(b, 0, read), "asdfjkl;");
    assertEquals(read = cis.read(b), -1);
    cis.close();
    assertEquals(cis.getVisibilities().toString(), "[A&B, B&C]");

    assertFalse(pi.hasNext());
  }

  public void testSingleByteRead() throws IOException {
    ChunkInputStream cis = new ChunkInputStream();
    PeekingIterator<Entry<Key,Value>> pi = new PeekingIterator<Entry<Key,Value>>(data.iterator());

    cis.setSource(pi);
    assertEquals((byte) 'a', (byte) cis.read());
    assertEquals((byte) 's', (byte) cis.read());
    assertEquals((byte) 'd', (byte) cis.read());
    assertEquals((byte) 'f', (byte) cis.read());
    assertEquals((byte) 'j', (byte) cis.read());
    assertEquals((byte) 'k', (byte) cis.read());
    assertEquals((byte) 'l', (byte) cis.read());
    assertEquals((byte) ';', (byte) cis.read());
    assertEquals(cis.read(), -1);
    cis.close();
    assertEquals(cis.getVisibilities().toString(), "[A&B]");
  }
}
