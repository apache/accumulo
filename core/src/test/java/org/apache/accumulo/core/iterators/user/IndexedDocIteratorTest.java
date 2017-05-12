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
package org.apache.accumulo.core.iterators.user;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFileTest;
import org.apache.accumulo.core.file.rfile.RFileTest.TestRFile;
import org.apache.accumulo.core.iterators.DefaultIteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class IndexedDocIteratorTest extends TestCase {

  private static final Logger log = Logger.getLogger(IndexedDocIteratorTest.class);

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  private static final byte[] nullByte = {0};

  private static IteratorEnvironment env = new DefaultIteratorEnvironment();

  TreeMap<Key,Value> map;
  Text[] columnFamilies;
  Text[] otherColumnFamilies;

  static int docid = 0;
  static String docColfPrefix = "doc";
  static Text indexColf = new Text("index");
  static Text docColf = new Text(docColfPrefix);

  static {
    log.setLevel(Level.OFF);
    docColf.append(nullByte, 0, 1);
    docColf.append("type".getBytes(), 0, "type".getBytes().length);
  }

  private TreeMap<Key,Value> createSortedMap(float hitRatio, int numRows, int numDocsPerRow, Text[] columnFamilies, Text[] otherColumnFamilies,
      HashSet<Text> docs, Text[] negatedColumns) {
    StringBuilder sb = new StringBuilder();
    Random r = new Random();
    Value v = new Value(new byte[0]);
    TreeMap<Key,Value> map = new TreeMap<>();
    boolean[] negateMask = new boolean[columnFamilies.length];

    for (int i = 0; i < columnFamilies.length; i++) {
      negateMask[i] = false;
      if (negatedColumns.length > 0)
        for (Text ng : negatedColumns)
          if (columnFamilies[i].equals(ng))
            negateMask[i] = true;
    }
    for (int i = 0; i < numRows; i++) {
      Text row = new Text(String.format("%06d", i));
      for (int startDocID = docid; docid - startDocID < numDocsPerRow; docid++) {
        sb.setLength(0);
        sb.append("fake doc contents");
        boolean docHits = true;
        Text doc = new Text("type");
        doc.append(nullByte, 0, 1);
        doc.append(String.format("%010d", docid).getBytes(), 0, 10);
        for (int j = 0; j < columnFamilies.length; j++) {
          if (r.nextFloat() < hitRatio) {
            Text colq = new Text(columnFamilies[j]);
            colq.append(nullByte, 0, 1);
            colq.append(doc.getBytes(), 0, doc.getLength());
            colq.append(nullByte, 0, 1);
            colq.append("stuff".getBytes(), 0, "stuff".length());
            Key k = new Key(row, indexColf, colq);
            map.put(k, v);
            sb.append(" ");
            sb.append(columnFamilies[j]);
            if (negateMask[j])
              docHits = false;
          } else {
            if (!negateMask[j])
              docHits = false;
          }
        }
        if (docHits) {
          docs.add(doc);
        }
        for (Text cf : otherColumnFamilies) {
          if (r.nextFloat() < hitRatio) {
            Text colq = new Text(cf);
            colq.append(nullByte, 0, 1);
            colq.append(doc.getBytes(), 0, doc.getLength());
            colq.append(nullByte, 0, 1);
            colq.append("stuff".getBytes(), 0, "stuff".length());
            Key k = new Key(row, indexColf, colq);
            map.put(k, v);
            sb.append(" ");
            sb.append(cf);
          }
        }
        sb.append(" docID=").append(doc);
        Key k = new Key(row, docColf, new Text(String.format("%010d", docid).getBytes()));
        map.put(k, new Value(sb.toString().getBytes()));
      }
    }
    return map;
  }

  static TestRFile trf = new TestRFile(DefaultConfiguration.getInstance());

  private SortedKeyValueIterator<Key,Value> createIteratorStack(float hitRatio, int numRows, int numDocsPerRow, Text[] columnFamilies,
      Text[] otherColumnFamilies, HashSet<Text> docs) throws IOException {
    Text nullText[] = new Text[0];
    return createIteratorStack(hitRatio, numRows, numDocsPerRow, columnFamilies, otherColumnFamilies, docs, nullText);
  }

  private SortedKeyValueIterator<Key,Value> createIteratorStack(float hitRatio, int numRows, int numDocsPerRow, Text[] columnFamilies,
      Text[] otherColumnFamilies, HashSet<Text> docs, Text[] negatedColumns) throws IOException {
    // write a map file
    trf.openWriter(false);

    TreeMap<Key,Value> inMemoryMap = createSortedMap(hitRatio, numRows, numDocsPerRow, columnFamilies, otherColumnFamilies, docs, negatedColumns);
    trf.writer.startNewLocalityGroup("docs", RFileTest.newColFamByteSequence(docColf.toString()));
    for (Entry<Key,Value> entry : inMemoryMap.entrySet()) {
      if (entry.getKey().getColumnFamily().equals(docColf))
        trf.writer.append(entry.getKey(), entry.getValue());
    }
    trf.writer.startNewLocalityGroup("terms", RFileTest.newColFamByteSequence(indexColf.toString()));
    for (Entry<Key,Value> entry : inMemoryMap.entrySet()) {
      if (entry.getKey().getColumnFamily().equals(indexColf))
        trf.writer.append(entry.getKey(), entry.getValue());
    }

    trf.closeWriter();

    trf.openReader();
    return trf.reader;
  }

  private synchronized static void cleanup() throws IOException {
    trf.closeReader();
    docid = 0;
  }

  public void testNull() {}

  @Override
  public void setUp() {
    Logger.getRootLogger().setLevel(Level.ERROR);
  }

  private static final int NUM_ROWS = 5;
  private static final int NUM_DOCIDS = 200;

  public void test1() throws IOException {
    columnFamilies = new Text[2];
    columnFamilies[0] = new Text("CC");
    columnFamilies[1] = new Text("EEE");
    otherColumnFamilies = new Text[4];
    otherColumnFamilies[0] = new Text("A");
    otherColumnFamilies[1] = new Text("B");
    otherColumnFamilies[2] = new Text("D");
    otherColumnFamilies[3] = new Text("F");

    float hitRatio = 0.5f;
    HashSet<Text> docs = new HashSet<>();
    SortedKeyValueIterator<Key,Value> source = createIteratorStack(hitRatio, NUM_ROWS, NUM_DOCIDS, columnFamilies, otherColumnFamilies, docs);
    IteratorSetting is = new IteratorSetting(1, IndexedDocIterator.class);
    IndexedDocIterator.setColumnFamilies(is, columnFamilies);
    IndexedDocIterator.setColfs(is, indexColf.toString(), docColfPrefix);
    IndexedDocIterator iter = new IndexedDocIterator();
    iter.init(source, is.getOptions(), env);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);
    int hitCount = 0;
    while (iter.hasTop()) {
      hitCount++;
      Key k = iter.getTopKey();
      Value v = iter.getTopValue();
      // System.out.println(k.toString());
      // System.out.println(iter.getDocID(k));

      Text d = IndexedDocIterator.parseDocID(k);
      assertTrue(docs.contains(d));
      assertTrue(new String(v.get()).endsWith(" docID=" + d));

      iter.next();
    }
    assertEquals(hitCount, docs.size());
    cleanup();
  }

  public void test2() throws IOException {
    columnFamilies = new Text[3];
    columnFamilies[0] = new Text("A");
    columnFamilies[1] = new Text("E");
    columnFamilies[2] = new Text("G");
    otherColumnFamilies = new Text[4];
    otherColumnFamilies[0] = new Text("B");
    otherColumnFamilies[1] = new Text("C");
    otherColumnFamilies[2] = new Text("D");
    otherColumnFamilies[3] = new Text("F");

    float hitRatio = 0.5f;
    HashSet<Text> docs = new HashSet<>();
    SortedKeyValueIterator<Key,Value> source = createIteratorStack(hitRatio, NUM_ROWS, NUM_DOCIDS, columnFamilies, otherColumnFamilies, docs);
    IteratorSetting is = new IteratorSetting(1, IndexedDocIterator.class);
    IndexedDocIterator.setColumnFamilies(is, columnFamilies);
    IndexedDocIterator.setColfs(is, indexColf.toString(), docColfPrefix);
    IndexedDocIterator iter = new IndexedDocIterator();
    iter.init(source, is.getOptions(), env);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);
    int hitCount = 0;
    while (iter.hasTop()) {
      hitCount++;
      Key k = iter.getTopKey();
      Value v = iter.getTopValue();
      Text d = IndexedDocIterator.parseDocID(k);
      assertTrue(docs.contains(d));
      assertTrue(new String(v.get()).endsWith(" docID=" + d));
      iter.next();
    }
    assertEquals(hitCount, docs.size());
    cleanup();
  }

  public void test3() throws IOException {
    columnFamilies = new Text[6];
    columnFamilies[0] = new Text("C");
    columnFamilies[1] = new Text("E");
    columnFamilies[2] = new Text("G");
    columnFamilies[3] = new Text("H");
    columnFamilies[4] = new Text("I");
    columnFamilies[5] = new Text("J");
    otherColumnFamilies = new Text[4];
    otherColumnFamilies[0] = new Text("A");
    otherColumnFamilies[1] = new Text("B");
    otherColumnFamilies[2] = new Text("D");
    otherColumnFamilies[3] = new Text("F");

    float hitRatio = 0.5f;
    HashSet<Text> docs = new HashSet<>();
    SortedKeyValueIterator<Key,Value> source = createIteratorStack(hitRatio, NUM_ROWS, NUM_DOCIDS, columnFamilies, otherColumnFamilies, docs);
    SortedKeyValueIterator<Key,Value> source2 = createIteratorStack(hitRatio, NUM_ROWS, NUM_DOCIDS, columnFamilies, otherColumnFamilies, docs);
    ArrayList<SortedKeyValueIterator<Key,Value>> sourceIters = new ArrayList<>();
    sourceIters.add(source);
    sourceIters.add(source2);
    MultiIterator mi = new MultiIterator(sourceIters, false);
    IteratorSetting is = new IteratorSetting(1, IndexedDocIterator.class);
    IndexedDocIterator.setColumnFamilies(is, columnFamilies);
    IndexedDocIterator.setColfs(is, indexColf.toString(), docColfPrefix);
    IndexedDocIterator iter = new IndexedDocIterator();
    iter.init(mi, is.getOptions(), env);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);
    int hitCount = 0;
    while (iter.hasTop()) {
      hitCount++;
      Key k = iter.getTopKey();
      Value v = iter.getTopValue();
      Text d = IndexedDocIterator.parseDocID(k);
      assertTrue(docs.contains(d));
      assertTrue(new String(v.get()).endsWith(" docID=" + d));
      iter.next();
    }
    assertEquals(hitCount, docs.size());
    cleanup();
  }

  public void test4() throws IOException {
    columnFamilies = new Text[3];
    boolean[] notFlags = new boolean[3];
    columnFamilies[0] = new Text("A");
    notFlags[0] = true;
    columnFamilies[1] = new Text("E");
    notFlags[1] = false;
    columnFamilies[2] = new Text("G");
    notFlags[2] = true;
    Text[] negatedColumns = new Text[2];
    negatedColumns[0] = new Text("A");
    negatedColumns[1] = new Text("G");
    otherColumnFamilies = new Text[4];
    otherColumnFamilies[0] = new Text("B");
    otherColumnFamilies[1] = new Text("C");
    otherColumnFamilies[2] = new Text("D");
    otherColumnFamilies[3] = new Text("F");

    float hitRatio = 0.5f;
    HashSet<Text> docs = new HashSet<>();
    SortedKeyValueIterator<Key,Value> source = createIteratorStack(hitRatio, NUM_ROWS, NUM_DOCIDS, columnFamilies, otherColumnFamilies, docs, negatedColumns);
    IteratorSetting is = new IteratorSetting(1, IndexedDocIterator.class);
    IndexedDocIterator.setColumnFamilies(is, columnFamilies, notFlags);
    IndexedDocIterator.setColfs(is, indexColf.toString(), docColfPrefix);
    IndexedDocIterator iter = new IndexedDocIterator();
    iter.init(source, is.getOptions(), env);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);
    int hitCount = 0;
    while (iter.hasTop()) {
      hitCount++;
      Key k = iter.getTopKey();
      Value v = iter.getTopValue();
      Text d = IndexedDocIterator.parseDocID(k);
      assertTrue(docs.contains(d));
      assertTrue(new String(v.get()).endsWith(" docID=" + d));
      iter.next();
    }
    assertTrue(hitCount == docs.size());
    cleanup();
  }
}
