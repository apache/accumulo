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
package org.apache.accumulo.core.file.rfile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RFileTest extends TestCase {
  
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
  
  static {
    Logger.getLogger(org.apache.hadoop.io.compress.CodecPool.class).setLevel(Level.OFF);
    Logger.getLogger(org.apache.hadoop.util.NativeCodeLoader.class).setLevel(Level.OFF);
  }
  
  static class SeekableByteArrayInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {
    
    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }
    
    @Override
    public long getPos() throws IOException {
      return pos;
    }
    
    @Override
    public void seek(long pos) throws IOException {
      if (mark != 0)
        throw new IllegalStateException();
      
      reset();
      long skipped = skip(pos);
      
      if (skipped != pos)
        throw new IOException();
    }
    
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
    
    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      
      if (position >= buf.length)
        throw new IllegalArgumentException();
      if (position + length > buf.length)
        throw new IllegalArgumentException();
      if (length > buffer.length)
        throw new IllegalArgumentException();
      
      System.arraycopy(buf, (int) position, buffer, offset, length);
      return length;
    }
    
    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      read(position, buffer, 0, buffer.length);
      
    }
    
    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      read(position, buffer, offset, length);
    }
    
  }
  
  private static void checkIndex(Reader reader) throws IOException {
    FileSKVIterator indexIter = reader.getIndex();
    
    if (indexIter.hasTop()) {
      Key lastKey = new Key(indexIter.getTopKey());
      
      if (reader.getFirstKey().compareTo(lastKey) > 0)
        throw new RuntimeException("First key out of order " + reader.getFirstKey() + " " + lastKey);
      
      indexIter.next();
      
      while (indexIter.hasTop()) {
        if (lastKey.compareTo(indexIter.getTopKey()) > 0)
          throw new RuntimeException("Indext out of order " + lastKey + " " + indexIter.getTopKey());
        
        lastKey = new Key(indexIter.getTopKey());
        indexIter.next();
        
      }
      
      if (reader.getLastKey().compareTo(lastKey) != 0) {
        throw new RuntimeException("Last key out of order " + reader.getLastKey() + " " + lastKey);
      }
    }
  }
  
  public static class TestRFile {
    
    private Configuration conf = CachedConfiguration.getInstance();
    public RFile.Writer writer;
    private ByteArrayOutputStream baos;
    private FSDataOutputStream dos;
    private SeekableByteArrayInputStream bais;
    private FSDataInputStream in;
    public Reader reader;
    public SortedKeyValueIterator<Key,Value> iter;

    public void openWriter(boolean startDLG) throws IOException {
      baos = new ByteArrayOutputStream();
      dos = new FSDataOutputStream(baos, new FileSystem.Statistics("a"));
      CachableBlockFile.Writer _cbw = new CachableBlockFile.Writer(dos, "gz", conf);
      writer = new RFile.Writer(_cbw, 1000, 1000);
      
      if (startDLG)
        writer.startDefaultLocalityGroup();
    }
    
    public void openWriter() throws IOException {
      openWriter(true);
    }
    
    public void closeWriter() throws IOException {
      writer.close();
      dos.close();
      baos.close();
    }
    
    public void openReader() throws IOException {
      byte[] data = baos.toByteArray();
      bais = new SeekableByteArrayInputStream(data);
      in = new FSDataInputStream(bais);
      CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(in, data.length, conf);
      reader = new RFile.Reader(_cbr);
      iter = new ColumnFamilySkippingIterator(reader);
      
      checkIndex(reader);
    }
    
    public void closeReader() throws IOException {
      reader.close();
      in.close();
    }
    
    public void seek(Key nk) throws IOException {
      iter.seek(new Range(nk, null), EMPTY_COL_FAMS, false);
    }
  }
  
  private Key nk(String row, String cf, String cq, String cv, long ts) {
    return new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), cv.getBytes(), ts);
  }
  
  private Value nv(String val) {
    return new Value(val.getBytes());
  }
  
  private String nf(String prefix, int i) {
    return String.format(prefix + "%06d", i);
  }
  
  public void test1() throws IOException {
    
    // test an emprt file
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter();
    trf.closeWriter();
    
    trf.openReader();
    trf.iter.seek(new Range((Key) null, null), EMPTY_COL_FAMS, false);
    assertFalse(trf.iter.hasTop());
    
    assertEquals(null, trf.reader.getLastKey());
    
    trf.closeReader();
  }
  
  public void test2() throws IOException {
    
    // test an rfile with one entry
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter();
    trf.writer.append(nk("r1", "cf1", "cq1", "L1", 55), nv("foo"));
    trf.closeWriter();
    
    trf.openReader();
    // seek before everything
    trf.seek(null);
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("r1", "cf1", "cq1", "L1", 55)));
    assertTrue(trf.iter.getTopValue().equals(nv("foo")));
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    // seek after the key
    trf.seek(nk("r2", "cf1", "cq1", "L1", 55));
    assertFalse(trf.iter.hasTop());
    
    // seek exactly to the key
    trf.seek(nk("r1", "cf1", "cq1", "L1", 55));
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("r1", "cf1", "cq1", "L1", 55)));
    assertTrue(trf.iter.getTopValue().equals(nv("foo")));
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    assertEquals(nk("r1", "cf1", "cq1", "L1", 55), trf.reader.getLastKey());
    
    trf.closeReader();
  }
  
  public void test3() throws IOException {
    
    // test an rfile with multiple rows having multiple columns
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter();
    
    int val = 0;
    
    ArrayList<Key> expectedKeys = new ArrayList<Key>(10000);
    ArrayList<Value> expectedValues = new ArrayList<Value>(10000);
    
    for (int row = 0; row < 4; row++) {
      String rowS = nf("r_", row);
      for (int cf = 0; cf < 4; cf++) {
        String cfS = nf("cf_", cf);
        for (int cq = 0; cq < 4; cq++) {
          String cqS = nf("cq_", cq);
          for (int cv = 'A'; cv < 'A' + 4; cv++) {
            String cvS = "" + (char) cv;
            for (int ts = 4; ts > 0; ts--) {
              Key k = nk(rowS, cfS, cqS, cvS, ts);
              k.setDeleted(true);
              Value v = nv("" + val);
              trf.writer.append(k, v);
              expectedKeys.add(k);
              expectedValues.add(v);
              
              k = nk(rowS, cfS, cqS, cvS, ts);
              v = nv("" + val);
              trf.writer.append(k, v);
              expectedKeys.add(k);
              expectedValues.add(v);
              
              val++;
            }
          }
        }
      }
    }
    
    // trf.writer.append(nk("r1","cf1","cq1","L1", 55), nv("foo"));
    trf.closeWriter();
    
    trf.openReader();
    // seek before everything
    trf.iter.seek(new Range((Key) null, null), EMPTY_COL_FAMS, false);
    verify(trf, expectedKeys.iterator(), expectedValues.iterator());
    
    // seek to the middle
    int index = expectedKeys.size() / 2;
    trf.seek(expectedKeys.get(index));
    verify(trf, expectedKeys.subList(index, expectedKeys.size()).iterator(), expectedValues.subList(index, expectedKeys.size()).iterator());
    
    // seek the first key
    index = 0;
    trf.seek(expectedKeys.get(index));
    verify(trf, expectedKeys.subList(index, expectedKeys.size()).iterator(), expectedValues.subList(index, expectedKeys.size()).iterator());
    
    // seek to the last key
    index = expectedKeys.size() - 1;
    trf.seek(expectedKeys.get(index));
    verify(trf, expectedKeys.subList(index, expectedKeys.size()).iterator(), expectedValues.subList(index, expectedKeys.size()).iterator());
    
    // seek after everything
    index = expectedKeys.size();
    trf.seek(new Key(new Text("z")));
    verify(trf, expectedKeys.subList(index, expectedKeys.size()).iterator(), expectedValues.subList(index, expectedKeys.size()).iterator());
    
    // test seeking to the current location
    index = expectedKeys.size() / 2;
    trf.seek(expectedKeys.get(index));
    assertTrue(trf.iter.hasTop());
    assertEquals(expectedKeys.get(index), trf.iter.getTopKey());
    assertEquals(expectedValues.get(index), trf.iter.getTopValue());
    
    trf.iter.next();
    index++;
    assertTrue(trf.iter.hasTop());
    assertEquals(expectedKeys.get(index), trf.iter.getTopKey());
    assertEquals(expectedValues.get(index), trf.iter.getTopValue());
    
    trf.seek(expectedKeys.get(index));
    
    assertTrue(trf.iter.hasTop());
    assertEquals(expectedKeys.get(index), trf.iter.getTopKey());
    assertEquals(expectedValues.get(index), trf.iter.getTopValue());
    
    // test seeking to each location in the file
    index = 0;
    for (Key key : expectedKeys) {
      trf.seek(key);
      assertTrue(trf.iter.hasTop());
      assertEquals(key, trf.iter.getTopKey());
      assertEquals(expectedValues.get(index), trf.iter.getTopValue());
      
      if (index > 0) {
        // Key pkey =
        expectedKeys.get(index - 1);
        // assertEquals(pkey, trf.reader.getPrevKey());
      }
      
      index++;
    }
    
    // test seeking backwards to each key
    for (int i = expectedKeys.size() - 1; i >= 0; i--) {
      Key key = expectedKeys.get(i);
      
      trf.seek(key);
      assertTrue(trf.iter.hasTop());
      assertEquals(key, trf.iter.getTopKey());
      assertEquals(expectedValues.get(i), trf.iter.getTopValue());
      
      if (i - 1 > 0) {
        // Key pkey =
        expectedKeys.get(i - 1);
        // assertEquals(pkey, trf.reader.getPrevKey());
      }
    }
    
    assertEquals(expectedKeys.get(expectedKeys.size() - 1), trf.reader.getLastKey());
    
    trf.closeReader();
  }
  
  private void verify(TestRFile trf, Iterator<Key> eki, Iterator<Value> evi) throws IOException {
    
    while (trf.iter.hasTop()) {
      Key ek = eki.next();
      Value ev = evi.next();
      
      assertEquals(ek, trf.iter.getTopKey());
      assertEquals(ev, trf.iter.getTopValue());
      
      trf.iter.next();
    }
    
    assertFalse(eki.hasNext());
    assertFalse(evi.hasNext());
  }
  
  public void test4() throws IOException {
    TestRFile trf = new TestRFile();
    
    trf.openWriter();
    
    trf.writer.append(nk("r1", "cf1", "cq1", "L1", 55), nv("foo1"));
    try {
      trf.writer.append(nk("r0", "cf1", "cq1", "L1", 55), nv("foo1"));
      assertFalse(true);
    } catch (IllegalStateException ioe) {
      
    }
    
    try {
      trf.writer.append(nk("r1", "cf0", "cq1", "L1", 55), nv("foo1"));
      assertFalse(true);
    } catch (IllegalStateException ioe) {
      
    }
    
    try {
      trf.writer.append(nk("r1", "cf1", "cq0", "L1", 55), nv("foo1"));
      assertFalse(true);
    } catch (IllegalStateException ioe) {
      
    }
    
    try {
      trf.writer.append(nk("r1", "cf1", "cq1", "L0", 55), nv("foo1"));
      assertFalse(true);
    } catch (IllegalStateException ioe) {
      
    }
    
    try {
      trf.writer.append(nk("r1", "cf1", "cq1", "L1", 56), nv("foo1"));
      assertFalse(true);
    } catch (IllegalStateException ioe) {
      
    }
  }
  
  public void test5() throws IOException {
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter();
    trf.writer.append(nk("r1", "cf1", "cq1", "L1", 55), nv("foo1"));
    trf.writer.append(nk("r1", "cf1", "cq4", "L1", 56), nv("foo2"));
    trf.closeWriter();
    
    trf.openReader();
    
    // test seeking between keys
    trf.seek(nk("r1", "cf1", "cq3", "L1", 55));
    assertTrue(trf.iter.hasTop());
    assertEquals(nk("r1", "cf1", "cq4", "L1", 56), trf.iter.getTopKey());
    assertEquals(nv("foo2"), trf.iter.getTopValue());
    
    // test seeking right before previous seek
    trf.seek(nk("r1", "cf1", "cq0", "L1", 55));
    assertTrue(trf.iter.hasTop());
    assertEquals(nk("r1", "cf1", "cq1", "L1", 55), trf.iter.getTopKey());
    assertEquals(nv("foo1"), trf.iter.getTopValue());
    
    assertEquals(nk("r1", "cf1", "cq4", "L1", 56), trf.reader.getLastKey());
    
    trf.closeReader();
  }
  
  public void test6() throws IOException {
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter();
    for (int i = 0; i < 500; i++) {
      trf.writer.append(nk(nf("r_", i), "cf1", "cq1", "L1", 55), nv("foo1"));
    }
    
    trf.closeWriter();
    
    trf.openReader();
    
    // repeatedly seek to locations before the first key in the file
    for (int i = 0; i < 10; i++) {
      trf.seek(nk(nf("q_", i), "cf1", "cq1", "L1", 55));
      assertTrue(trf.iter.hasTop());
      assertEquals(nk(nf("r_", 0), "cf1", "cq1", "L1", 55), trf.iter.getTopKey());
      assertEquals(nv("foo1"), trf.iter.getTopValue());
    }
    
    // repeatedly seek to locations after the last key in the file
    for (int i = 0; i < 10; i++) {
      trf.seek(nk(nf("s_", i), "cf1", "cq1", "L1", 55));
      assertFalse(trf.iter.hasTop());
    }
    
    assertEquals(nk(nf("r_", 499), "cf1", "cq1", "L1", 55), trf.reader.getLastKey());
    
    trf.closeReader();
  }
  
  public void test7() throws IOException {
    // these test excercise setting the end key of a range
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter();
    for (int i = 2; i < 50; i++) {
      trf.writer.append(nk(nf("r_", i), "cf1", "cq1", "L1", 55), nv("foo" + i));
    }
    
    trf.closeWriter();
    
    trf.openReader();
    
    // test that has top returns false when end of range reached
    trf.iter.seek(new Range(nk(nf("r_", 3), "cf1", "cq1", "L1", 55), true, nk(nf("r_", 4), "cf1", "cq1", "L1", 55), false), EMPTY_COL_FAMS, false);
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk(nf("r_", 3), "cf1", "cq1", "L1", 55)));
    assertEquals(nv("foo" + 3), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    // test seeking to a range that is between two keys, should not return anything
    trf.iter.seek(new Range(nk(nf("r_", 4) + "a", "cf1", "cq1", "L1", 55), true, nk(nf("r_", 4) + "b", "cf1", "cq1", "L1", 55), true), EMPTY_COL_FAMS, false);
    assertFalse(trf.iter.hasTop());
    
    // test seeking to another range after the previously seeked range, that is between the same two keys in the file
    // as the previously seeked range.... this test an optimization on RFile
    trf.iter.seek(new Range(nk(nf("r_", 4) + "c", "cf1", "cq1", "L1", 55), true, nk(nf("r_", 4) + "d", "cf1", "cq1", "L1", 55), true), EMPTY_COL_FAMS, false);
    assertFalse(trf.iter.hasTop());
    
    trf.iter.seek(new Range(nk(nf("r_", 4) + "e", "cf1", "cq1", "L1", 55), true, nk(nf("r_", 4) + "f", "cf1", "cq1", "L1", 55), true), EMPTY_COL_FAMS, false);
    assertFalse(trf.iter.hasTop());
    
    // now ensure we can seek somewhere, that triggering the optimization does not cause any problems
    trf.iter.seek(new Range(nk(nf("r_", 5), "cf1", "cq1", "L1", 55), true, nk(nf("r_", 6), "cf1", "cq1", "L1", 55), false), EMPTY_COL_FAMS, false);
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk(nf("r_", 5), "cf1", "cq1", "L1", 55)));
    assertEquals(nv("foo" + 5), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    // test seeking to range that is before the beginning of the file
    trf.iter.seek(new Range(nk(nf("r_", 0), "cf1", "cq1", "L1", 55), true, nk(nf("r_", 2), "cf1", "cq1", "L1", 55), false), EMPTY_COL_FAMS, false);
    assertFalse(trf.iter.hasTop());
    
    assertEquals(nk(nf("r_", 49), "cf1", "cq1", "L1", 55), trf.reader.getLastKey());
    
    trf.reader.close();
  }
  
  public void test8() throws IOException {
    TestRFile trf = new TestRFile();
    
    trf.openWriter();
    
    for (int i = 0; i < 2500; i++) {
      trf.writer.append(nk(nf("r_", i), "cf1", "cq1", "L1", 42), nv("foo" + i));
    }
    
    trf.closeWriter();
    trf.openReader();
    
    // test seeking between each key forward
    for (int i = 0; i < 2499; i++) {
      trf.seek(nk(nf("r_", i), "cf1", "cq1", "L1", 42).followingKey(PartialKey.ROW));
      assertTrue(trf.iter.hasTop());
      assertEquals(nk(nf("r_", i + 1), "cf1", "cq1", "L1", 42), trf.iter.getTopKey());
    }
    
    // test seeking between each key forward
    for (int i = 0; i < 2499; i += 2) {
      trf.seek(nk(nf("r_", i), "cf1", "cq1", "L1", 42).followingKey(PartialKey.ROW));
      assertTrue(trf.iter.hasTop());
      assertEquals(nk(nf("r_", i + 1), "cf1", "cq1", "L1", 42), trf.iter.getTopKey());
    }
    
    // test seeking backwards between each key
    for (int i = 2498; i >= 0; i--) {
      trf.seek(nk(nf("r_", i), "cf1", "cq1", "L1", 42).followingKey(PartialKey.ROW));
      assertTrue(trf.iter.hasTop());
      assertEquals(nk(nf("r_", i + 1), "cf1", "cq1", "L1", 42), trf.iter.getTopKey());
    }
    
    trf.closeReader();
    
    // do same test with col fam
    trf = new TestRFile();
    
    trf.openWriter();
    
    for (int i = 0; i < 2500; i++) {
      trf.writer.append(nk(nf("r_", 0), nf("cf_", i), "cq1", "L1", 42), nv("foo" + i));
    }
    
    trf.closeWriter();
    trf.openReader();
    
    // test seeking between each key forward
    for (int i = 0; i < 2499; i++) {
      trf.seek(nk(nf("r_", 0), nf("cf_", i), "cq1", "L1", 42).followingKey(PartialKey.ROW_COLFAM));
      assertTrue(trf.iter.hasTop());
      assertEquals(nk(nf("r_", 0), nf("cf_", i + 1), "cq1", "L1", 42), trf.iter.getTopKey());
    }
    
    // test seeking between each key forward
    for (int i = 0; i < 2499; i += 2) {
      trf.seek(nk(nf("r_", 0), nf("cf_", i), "cq1", "L1", 42).followingKey(PartialKey.ROW_COLFAM));
      assertTrue(trf.iter.hasTop());
      assertEquals(nk(nf("r_", 0), nf("cf_", i + 1), "cq1", "L1", 42), trf.iter.getTopKey());
    }
    
    // test seeking backwards between each key
    for (int i = 2498; i >= 0; i--) {
      trf.seek(nk(nf("r_", 0), nf("cf_", i), "cq1", "L1", 42).followingKey(PartialKey.ROW_COLFAM));
      assertTrue(trf.iter.hasTop());
      assertEquals(nk(nf("r_", 0), nf("cf_", i + 1), "cq1", "L1", 42), trf.iter.getTopKey());
    }
    
    trf.closeReader();
    
    // do same test with col qual
    trf = new TestRFile();
    
    trf.openWriter();
    
    for (int i = 0; i < 2500; i++) {
      trf.writer.append(nk(nf("r_", 0), nf("cf_", 0), nf("cq_", i), "L1", 42), nv("foo" + i));
    }
    
    trf.closeWriter();
    trf.openReader();
    
    // test seeking between each key forward
    for (int i = 0; i < 2499; i++) {
      trf.seek(nk(nf("r_", 0), nf("cf_", 0), nf("cq_", i), "L1", 42).followingKey(PartialKey.ROW_COLFAM_COLQUAL));
      assertTrue(trf.iter.hasTop());
      assertEquals(nk(nf("r_", 0), nf("cf_", 0), nf("cq_", i + 1), "L1", 42), trf.iter.getTopKey());
    }
    
    // test seeking between each key forward
    for (int i = 0; i < 2499; i += 2) {
      trf.seek(nk(nf("r_", 0), nf("cf_", 0), nf("cq_", i), "L1", 42).followingKey(PartialKey.ROW_COLFAM_COLQUAL));
      assertTrue(trf.iter.hasTop());
      assertEquals(nk(nf("r_", 0), nf("cf_", 0), nf("cq_", i + 1), "L1", 42), trf.iter.getTopKey());
    }
    
    // test seeking backwards between each key
    for (int i = 2498; i >= 0; i--) {
      trf.seek(nk(nf("r_", 0), nf("cf_", 0), nf("cq_", i), "L1", 42).followingKey(PartialKey.ROW_COLFAM_COLQUAL));
      assertTrue(trf.iter.hasTop());
      assertEquals(nk(nf("r_", 0), nf("cf_", 0), nf("cq_", i + 1), "L1", 42), trf.iter.getTopKey());
    }
    
    trf.closeReader();
  }
  
  public static Set<ByteSequence> ncfs(String... colFams) {
    HashSet<ByteSequence> cfs = new HashSet<ByteSequence>();
    
    for (String cf : colFams) {
      cfs.add(new ArrayByteSequence(cf));
    }
    
    return cfs;
  }
  
  public void test9() throws IOException {
    TestRFile trf = new TestRFile();
    
    trf.openWriter(false);
    
    trf.writer.startNewLocalityGroup("lg1", ncfs("cf1", "cf2"));
    
    trf.writer.append(nk("0000", "cf1", "doe,john", "", 4), nv("1123 West Left st"));
    trf.writer.append(nk("0002", "cf2", "doe,jane", "", 5), nv("1124 East Right st"));
    
    trf.writer.startNewLocalityGroup("lg2", ncfs("cf3", "cf4"));
    
    trf.writer.append(nk("0001", "cf3", "buck,john", "", 4), nv("90 Slum st"));
    trf.writer.append(nk("0003", "cf4", "buck,jane", "", 5), nv("09 Slum st"));
    
    trf.writer.close();
    
    trf.openReader();
    
    // scan first loc group
    Range r = new Range(nk("0000", "cf1", "doe,john", "", 4), true, nk("0003", "cf4", "buck,jane", "", 5), true);
    trf.iter.seek(r, ncfs("cf1", "cf2"), true);
    assertEquals(1, trf.reader.getNumLocalityGroupsSeeked());
    
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0000", "cf1", "doe,john", "", 4)));
    assertEquals(nv("1123 West Left st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0002", "cf2", "doe,jane", "", 5)));
    assertEquals(nv("1124 East Right st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    // scan second loc group
    r = new Range(nk("0000", "cf1", "doe,john", "", 4), true, nk("0003", "cf4", "buck,jane", "", 5), true);
    trf.iter.seek(r, ncfs("cf3", "cf4"), true);
    assertEquals(1, trf.reader.getNumLocalityGroupsSeeked());
    
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0001", "cf3", "buck,john", "", 4)));
    assertEquals(nv("90 Slum st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0003", "cf4", "buck,jane", "", 5)));
    assertEquals(nv("09 Slum st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    // scan all loc groups
    r = new Range(nk("0000", "cf1", "doe,john", "", 4), true, nk("0003", "cf4", "buck,jane", "", 5), true);
    trf.iter.seek(r, EMPTY_COL_FAMS, false);
    assertEquals(2, trf.reader.getNumLocalityGroupsSeeked());
    
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0000", "cf1", "doe,john", "", 4)));
    assertEquals(nv("1123 West Left st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0001", "cf3", "buck,john", "", 4)));
    assertEquals(nv("90 Slum st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0002", "cf2", "doe,jane", "", 5)));
    assertEquals(nv("1124 East Right st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0003", "cf4", "buck,jane", "", 5)));
    assertEquals(nv("09 Slum st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    // scan no loc groups
    r = new Range(nk("0000", "cf1", "doe,john", "", 4), true, nk("0003", "cf4", "buck,jane", "", 5), true);
    trf.iter.seek(r, ncfs("saint", "dogooder"), true);
    assertEquals(0, trf.reader.getNumLocalityGroupsSeeked());
    assertFalse(trf.iter.hasTop());
    
    // scan a subset of second locality group
    r = new Range(nk("0000", "cf1", "doe,john", "", 4), true, nk("0003", "cf4", "buck,jane", "", 5), true);
    trf.iter.seek(r, ncfs("cf4"), true);
    assertEquals(1, trf.reader.getNumLocalityGroupsSeeked());
    
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0003", "cf4", "buck,jane", "", 5)));
    assertEquals(nv("09 Slum st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    // scan a subset of second locality group
    r = new Range(nk("0000", "cf1", "doe,john", "", 4), true, nk("0003", "cf4", "buck,jane", "", 5), true);
    trf.iter.seek(r, ncfs("cf3"), true);
    assertEquals(1, trf.reader.getNumLocalityGroupsSeeked());
    
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0001", "cf3", "buck,john", "", 4)));
    assertEquals(nv("90 Slum st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    // scan subset of first loc group
    r = new Range(nk("0000", "cf1", "doe,john", "", 4), true, nk("0003", "cf4", "buck,jane", "", 5), true);
    trf.iter.seek(r, ncfs("cf1"), true);
    assertEquals(1, trf.reader.getNumLocalityGroupsSeeked());
    
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0000", "cf1", "doe,john", "", 4)));
    assertEquals(nv("1123 West Left st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    // scan subset of first loc group
    r = new Range(nk("0000", "cf1", "doe,john", "", 4), true, nk("0003", "cf4", "buck,jane", "", 5), true);
    trf.iter.seek(r, ncfs("cf2"), true);
    assertEquals(1, trf.reader.getNumLocalityGroupsSeeked());
    
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0002", "cf2", "doe,jane", "", 5)));
    assertEquals(nv("1124 East Right st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    // scan subset of all loc groups
    r = new Range(nk("0000", "cf1", "doe,john", "", 4), true, nk("0003", "cf4", "buck,jane", "", 5), true);
    trf.iter.seek(r, ncfs("cf1", "cf4"), true);
    assertEquals(2, trf.reader.getNumLocalityGroupsSeeked());
    
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0000", "cf1", "doe,john", "", 4)));
    assertEquals(nv("1123 West Left st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0003", "cf4", "buck,jane", "", 5)));
    assertEquals(nv("09 Slum st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    trf.closeReader();
    
  }
  
  public void test10() throws IOException {
    
    // test empty locality groups
    TestRFile trf = new TestRFile();
    
    trf.openWriter(false);
    trf.writer.startNewLocalityGroup("lg1", ncfs("cf1", "cf2"));
    trf.writer.startNewLocalityGroup("lg2", ncfs("cf3", "cf4"));
    trf.writer.startDefaultLocalityGroup();
    trf.writer.close();
    
    trf.openReader();
    trf.iter.seek(new Range(new Text(""), null), EMPTY_COL_FAMS, false);
    assertFalse(trf.iter.hasTop());
    
    trf.closeReader();
    
    // another empty locality group test
    trf = new TestRFile();
    
    trf.openWriter(false);
    trf.writer.startNewLocalityGroup("lg1", ncfs("cf1", "cf2"));
    trf.writer.append(nk("0000", "cf1", "doe,john", "", 4), nv("1123 West Left st"));
    trf.writer.append(nk("0002", "cf2", "doe,jane", "", 5), nv("1124 East Right st"));
    trf.writer.startNewLocalityGroup("lg2", ncfs("cf3", "cf4"));
    trf.writer.startDefaultLocalityGroup();
    trf.writer.close();
    
    trf.openReader();
    trf.iter.seek(new Range(new Text(""), null), EMPTY_COL_FAMS, false);
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0000", "cf1", "doe,john", "", 4)));
    assertEquals(nv("1123 West Left st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0002", "cf2", "doe,jane", "", 5)));
    assertEquals(nv("1124 East Right st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    trf.closeReader();
    
    // another empty locality group test
    trf = new TestRFile();
    
    trf.openWriter(false);
    trf.writer.startNewLocalityGroup("lg1", ncfs("cf1", "cf2"));
    trf.writer.startNewLocalityGroup("lg2", ncfs("cf3", "cf4"));
    trf.writer.append(nk("0001", "cf3", "buck,john", "", 4), nv("90 Slum st"));
    trf.writer.append(nk("0003", "cf4", "buck,jane", "", 5), nv("09 Slum st"));
    trf.writer.startDefaultLocalityGroup();
    trf.writer.close();
    
    trf.openReader();
    trf.iter.seek(new Range(new Text(""), null), EMPTY_COL_FAMS, false);
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0001", "cf3", "buck,john", "", 4)));
    assertEquals(nv("90 Slum st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0003", "cf4", "buck,jane", "", 5)));
    assertEquals(nv("09 Slum st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    trf.closeReader();
    
    // another empty locality group test
    trf = new TestRFile();
    
    trf.openWriter(false);
    trf.writer.startNewLocalityGroup("lg1", ncfs("cf1", "cf2"));
    trf.writer.startNewLocalityGroup("lg2", ncfs("cf3", "cf4"));
    trf.writer.startDefaultLocalityGroup();
    trf.writer.append(nk("0007", "good citizen", "q,john", "", 4), nv("70 Apple st"));
    trf.writer.append(nk("0008", "model citizen", "q,jane", "", 5), nv("81 Plum st"));
    trf.writer.close();
    
    trf.openReader();
    trf.iter.seek(new Range(new Text(""), null), EMPTY_COL_FAMS, false);
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0007", "good citizen", "q,john", "", 4)));
    assertEquals(nv("70 Apple st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0008", "model citizen", "q,jane", "", 5)));
    assertEquals(nv("81 Plum st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    trf.closeReader();
    
    // another empty locality group test
    trf = new TestRFile();
    
    trf.openWriter(false);
    trf.writer.startNewLocalityGroup("lg1", ncfs("cf1", "cf2"));
    trf.writer.append(nk("0000", "cf1", "doe,john", "", 4), nv("1123 West Left st"));
    trf.writer.append(nk("0002", "cf2", "doe,jane", "", 5), nv("1124 East Right st"));
    trf.writer.startNewLocalityGroup("lg2", ncfs("cf3", "cf4"));
    trf.writer.startDefaultLocalityGroup();
    trf.writer.append(nk("0007", "good citizen", "q,john", "", 4), nv("70 Apple st"));
    trf.writer.append(nk("0008", "model citizen", "q,jane", "", 5), nv("81 Plum st"));
    trf.writer.close();
    
    trf.openReader();
    trf.iter.seek(new Range(new Text(""), null), EMPTY_COL_FAMS, false);
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0000", "cf1", "doe,john", "", 4)));
    assertEquals(nv("1123 West Left st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0002", "cf2", "doe,jane", "", 5)));
    assertEquals(nv("1124 East Right st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0007", "good citizen", "q,john", "", 4)));
    assertEquals(nv("70 Apple st"), trf.iter.getTopValue());
    trf.iter.next();
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0008", "model citizen", "q,jane", "", 5)));
    assertEquals(nv("81 Plum st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    trf.closeReader();
  }
  
  public void test11() throws IOException {
    // test locality groups with more than two entries
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter(false);
    trf.writer.startNewLocalityGroup("lg1", ncfs("3mod10"));
    for (int i = 3; i < 1024; i += 10) {
      trf.writer.append(nk(nf("i", i), "3mod10", "", "", i + 2), nv("" + i));
    }
    
    trf.writer.startNewLocalityGroup("lg2", ncfs("5mod10", "7mod10"));
    
    for (int i = 5; i < 1024;) {
      trf.writer.append(nk(nf("i", i), "5mod10", "", "", i + 2), nv("" + i));
      i += 2;
      trf.writer.append(nk(nf("i", i), "7mod10", "", "", i + 2), nv("" + i));
      i += 8;
    }
    
    trf.writer.startDefaultLocalityGroup();
    for (int i = 0; i < 1024; i++) {
      
      int m10 = i % 10;
      if (m10 == 3 || m10 == 5 || m10 == 7)
        continue;
      
      trf.writer.append(nk(nf("i", i), m10 + "mod10", "", "", i + 2), nv("" + i));
      
    }
    trf.writer.close();
    
    // test a merged read of all column families
    trf.openReader();
    trf.iter.seek(new Range(new Text(""), null), EMPTY_COL_FAMS, false);
    assertEquals(3, trf.reader.getNumLocalityGroupsSeeked());
    for (int i = 0; i < 1024; i++) {
      assertTrue(trf.iter.hasTop());
      assertEquals(nk(nf("i", i), (i % 10) + "mod10", "", "", i + 2), trf.iter.getTopKey());
      assertEquals(nv("" + i), trf.iter.getTopValue());
      trf.iter.next();
    }
    assertFalse(trf.iter.hasTop());
    
    // try reading each of the 10 column families separately
    for (int m = 0; m < 10; m++) {
      trf.iter.seek(new Range(new Key(), true, null, true), ncfs(m + "mod10"), true);
      assertEquals(1, trf.reader.getNumLocalityGroupsSeeked());
      for (int i = m; i < 1024; i += 10) {
        assertTrue(trf.iter.hasTop());
        assertEquals(nk(nf("i", i), (i % 10) + "mod10", "", "", i + 2), trf.iter.getTopKey());
        assertEquals(nv("" + i), trf.iter.getTopValue());
        trf.iter.next();
      }
      assertFalse(trf.iter.hasTop());
      
      // test excluding an individual column family
      trf.iter.seek(new Range(new Key(), true, null, true), ncfs(m + "mod10"), false);
      if (m == 3)
        assertEquals(2, trf.reader.getNumLocalityGroupsSeeked());
      else
        assertEquals(3, trf.reader.getNumLocalityGroupsSeeked());
      for (int i = 0; i < 1024; i++) {
        
        if (i % 10 == m)
          continue;
        
        assertTrue(trf.iter.hasTop());
        assertEquals(nk(nf("i", i), (i % 10) + "mod10", "", "", i + 2), trf.iter.getTopKey());
        assertEquals(nv("" + i), trf.iter.getTopValue());
        trf.iter.next();
      }
      assertFalse(trf.iter.hasTop());
    }
    
    // test Rfile deepcopy
    SortedKeyValueIterator<Key,Value> reader2 = trf.iter.deepCopy(null);
    
    // try reading from cloned reader at the same time as parent reader
    for (int m = 0; m < 9; m++) {
      trf.iter.seek(new Range(new Key(), true, null, true), ncfs(m + "mod10"), true);
      assertEquals(1, trf.reader.getNumLocalityGroupsSeeked());
      reader2.seek(new Range(new Key(), true, null, true), ncfs((m + 1) + "mod10"), true);
      // assertEquals(1, reader2.getNumLocalityGroupsSeeked());
      for (int i = m; i < 1024; i += 10) {
        // System.out.println(m+","+i);
        assertTrue(trf.iter.hasTop());
        assertEquals(nk(nf("i", i), (i % 10) + "mod10", "", "", i + 2), trf.iter.getTopKey());
        assertEquals(nv("" + i), trf.iter.getTopValue());
        trf.iter.next();
        if (i + 1 < 1024) {
          assertTrue(reader2.hasTop());
          assertEquals(nk(nf("i", (i + 1)), ((i + 1) % 10) + "mod10", "", "", i + 3), reader2.getTopKey());
          assertEquals(nv("" + (i + 1)), reader2.getTopValue());
          reader2.next();
        }
      }
      assertFalse(trf.iter.hasTop());
      assertFalse(reader2.hasTop());
    }
    
    trf.closeReader();
  }
  
  public void test12() throws IOException {
    // test inserting column fams not in locality groups
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter(false);
    
    trf.writer.startNewLocalityGroup("lg1", ncfs("a", "b"));
    
    trf.writer.append(nk("0007", "a", "cq1", "", 4), nv("1"));
    
    try {
      trf.writer.append(nk("0009", "c", "cq1", "", 4), nv("1"));
      assertFalse(true);
    } catch (IllegalArgumentException ioe) {
      
    }
    
    trf.closeWriter();
    
    trf.openReader();
    
    trf.iter.seek(new Range(), EMPTY_COL_FAMS, false);
    assertTrue(trf.iter.hasTop());
    assertEquals(nk("0007", "a", "cq1", "", 4), trf.iter.getTopKey());
    assertEquals(nv("1"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
  }
  
  public void test13() throws IOException {
    // test inserting column fam in default loc group that was in
    // previous locality group
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter(false);
    
    trf.writer.startNewLocalityGroup("lg1", ncfs("a", "b"));
    
    trf.writer.append(nk("0007", "a", "cq1", "", 4), nv("1"));
    
    trf.writer.startDefaultLocalityGroup();
    
    try {
      trf.writer.append(nk("0008", "a", "cq1", "", 4), nv("1"));
      assertFalse(true);
    } catch (IllegalArgumentException ioe) {
      
    }
    
    try {
      trf.writer.append(nk("0009", "b", "cq1", "", 4), nv("1"));
      assertFalse(true);
    } catch (IllegalArgumentException ioe) {
      
    }
    
    trf.closeWriter();
    
    trf.openReader();
    
    trf.iter.seek(new Range(), EMPTY_COL_FAMS, false);
    assertTrue(trf.iter.hasTop());
    assertEquals(nk("0007", "a", "cq1", "", 4), trf.iter.getTopKey());
    assertEquals(nv("1"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
  }
  
  public void test14() throws IOException {
    // test starting locality group after default locality group was started
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter(false);
    
    trf.writer.startDefaultLocalityGroup();
    try {
      trf.writer.startNewLocalityGroup("lg1", ncfs("a", "b"));
      assertFalse(true);
    } catch (IllegalStateException ioe) {
      
    }
    
    try {
      trf.writer.startDefaultLocalityGroup();
      assertFalse(true);
    } catch (IllegalStateException ioe) {
      
    }
    
    trf.writer.close();
  }
  
  public void test16() throws IOException {
    TestRFile trf = new TestRFile();
    
    trf.openWriter(false);
    
    trf.writer.startNewLocalityGroup("lg1", ncfs("a", "b"));
    
    trf.writer.append(nk("0007", "a", "cq1", "", 4), nv("1"));
    try {
      trf.writer.startNewLocalityGroup("lg1", ncfs("b", "c"));
      assertFalse(true);
    } catch (IllegalArgumentException ioe) {
      
    }
    
    trf.closeWriter();
  }
  
  public void test17() throws IOException {
    // add alot of the same keys to rfile that cover multiple blocks...
    // this should cause the keys in the index to be exactly the same...
    // ensure seeks work correctly
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter(false);
    
    trf.writer.startDefaultLocalityGroup();
    for (int i = 0; i < 2048; i++) {
      trf.writer.append(nk("r0000", "cf1", "cq1", "", 1), nv("" + i));
    }
    
    for (int i = 2048; i < 4096; i++) {
      trf.writer.append(nk("r0001", "cf1", "cq1", "", 1), nv("" + i));
    }
    
    trf.writer.close();
    
    trf.openReader();
    
    FileSKVIterator indexIter = trf.reader.getIndex();
    int count = 0;
    while (indexIter.hasTop()) {
      count++;
      indexIter.next();
    }
    
    assert (count > 4);
    
    trf.iter.seek(new Range(nk("r0000", "cf1", "cq1", "", 1), true, nk("r0001", "cf1", "cq1", "", 1), false), EMPTY_COL_FAMS, false);
    
    for (int i = 0; i < 2048; i++) {
      assertTrue(trf.iter.hasTop());
      assertEquals(nk("r0000", "cf1", "cq1", "", 1), trf.iter.getTopKey());
      assertEquals(nv("" + i), trf.iter.getTopValue());
      trf.iter.next();
    }
    
    assertFalse(trf.iter.hasTop());
    
    trf.iter.seek(new Range(nk("r0000", "cf1", "cq1", "", 1), false, nk("r0001", "cf1", "cq1", "", 1), true), EMPTY_COL_FAMS, false);
    
    for (int i = 2048; i < 4096; i++) {
      assertTrue(trf.iter.hasTop());
      assertEquals(nk("r0001", "cf1", "cq1", "", 1), trf.iter.getTopKey());
      assertEquals(nv("" + i), trf.iter.getTopValue());
      trf.iter.next();
    }
    
    assertFalse(trf.iter.hasTop());
    
    trf.iter.seek(new Range(nk("r0001", "cf1", "cq1", "", 1), true, nk("r0001", "cf1", "cq1", "", 1), true), EMPTY_COL_FAMS, false);
    
    for (int i = 2048; i < 4096; i++) {
      assertTrue(trf.iter.hasTop());
      assertEquals(nk("r0001", "cf1", "cq1", "", 1), trf.iter.getTopKey());
      assertEquals(nv("" + i), trf.iter.getTopValue());
      trf.iter.next();
    }
    
    assertFalse(trf.iter.hasTop());
    
    trf.iter.seek(new Range(nk("r0002", "cf1", "cq1", "", 1), true, nk("r0002", "cf1", "cq1", "", 1), true), EMPTY_COL_FAMS, false);
    assertFalse(trf.iter.hasTop());
    
    trf.iter.seek(new Range((Key) null, null), EMPTY_COL_FAMS, false);
    
    for (int i = 0; i < 2048; i++) {
      assertTrue(trf.iter.hasTop());
      assertEquals(nk("r0000", "cf1", "cq1", "", 1), trf.iter.getTopKey());
      assertEquals(nv("" + i), trf.iter.getTopValue());
      trf.iter.next();
    }
    
    for (int i = 2048; i < 4096; i++) {
      assertTrue(trf.iter.hasTop());
      assertEquals(nk("r0001", "cf1", "cq1", "", 1), trf.iter.getTopKey());
      assertEquals(nv("" + i), trf.iter.getTopValue());
      trf.iter.next();
    }
    
    assertFalse(trf.iter.hasTop());
    
    trf.closeReader();
  }
  
  private String t18ncf(int i) {
    return String.format("cf%06d", i);
  }
  
  private Set<ByteSequence> t18ncfs(int... colFams) {
    HashSet<ByteSequence> cfs = new HashSet<ByteSequence>();
    for (int i : colFams) {
      cfs.add(new ArrayByteSequence(t18ncf(i)));
    }
    
    return cfs;
  }
  
  private void t18Append(TestRFile trf, HashSet<ByteSequence> allCf, int i) throws IOException {
    String cf = t18ncf(i);
    trf.writer.append(nk("r0000", cf, "cq1", "", 1), nv("" + i));
    allCf.add(new ArrayByteSequence(cf));
  }
  
  private void t18Verify(Set<ByteSequence> cfs, SortedKeyValueIterator<Key,Value> iter, Reader reader, HashSet<ByteSequence> allCf, int eialg, int eealg)
      throws IOException {
    
    HashSet<ByteSequence> colFamsSeen = new HashSet<ByteSequence>();
    
    iter.seek(new Range(), cfs, true);
    assertEquals(eialg, reader.getNumLocalityGroupsSeeked());
    
    while (iter.hasTop()) {
      colFamsSeen.add(iter.getTopKey().getColumnFamilyData());
      iter.next();
    }
    
    HashSet<ByteSequence> expected = new HashSet<ByteSequence>(allCf);
    expected.retainAll(cfs);
    assertEquals(expected, colFamsSeen);
    
    iter.seek(new Range(), cfs, false);
    assertEquals(eealg, reader.getNumLocalityGroupsSeeked());
    
    colFamsSeen.clear();
    while (iter.hasTop()) {
      colFamsSeen.add(iter.getTopKey().getColumnFamilyData());
      iter.next();
    }
    
    HashSet<ByteSequence> nonExcluded = new HashSet<ByteSequence>(allCf);
    nonExcluded.removeAll(cfs);
    assertEquals(nonExcluded, colFamsSeen);
  }
  
  public void test18() throws IOException {
    // test writing more column families to default LG than it will track
    
    TestRFile trf = new TestRFile();
    
    trf.openWriter(false);
    
    HashSet<ByteSequence> allCf = new HashSet<ByteSequence>();
    
    trf.writer.startNewLocalityGroup("lg1", t18ncfs(0));
    for (int i = 0; i < 1; i++)
      t18Append(trf, allCf, i);
    
    trf.writer.startNewLocalityGroup("lg2", t18ncfs(1, 2));
    for (int i = 1; i < 3; i++)
      t18Append(trf, allCf, i);
    
    trf.writer.startNewLocalityGroup("lg3", t18ncfs(3, 4, 5));
    for (int i = 3; i < 6; i++)
      t18Append(trf, allCf, i);
    
    trf.writer.startDefaultLocalityGroup();
    
    int max = 6 + RFile.Writer.MAX_CF_IN_DLG + 100;
    for (int i = 6; i < max; i++)
      t18Append(trf, allCf, i);
    
    trf.closeWriter();
    
    trf.openReader();
    
    t18Verify(t18ncfs(0), trf.iter, trf.reader, allCf, 1, 3);
    for (int i = 1; i < 10; i++)
      t18Verify(t18ncfs(i), trf.iter, trf.reader, allCf, 1, 4);
    
    t18Verify(t18ncfs(max + 1), trf.iter, trf.reader, allCf, 1, 4);
    
    t18Verify(t18ncfs(1, 2, 3, 4), trf.iter, trf.reader, allCf, 2, 3);
    t18Verify(t18ncfs(1, 2, 3, 4, 5), trf.iter, trf.reader, allCf, 2, 2);
    
    t18Verify(t18ncfs(0, 1, 2, 3, 4), trf.iter, trf.reader, allCf, 3, 2);
    t18Verify(t18ncfs(0, 1, 2, 3, 4, 5), trf.iter, trf.reader, allCf, 3, 1);
    t18Verify(t18ncfs(0, 1, 2, 3, 4, 5, 6), trf.iter, trf.reader, allCf, 4, 1);
    
    t18Verify(t18ncfs(0, 1), trf.iter, trf.reader, allCf, 2, 3);
    t18Verify(t18ncfs(2, 3), trf.iter, trf.reader, allCf, 2, 4);
    t18Verify(t18ncfs(5, 6), trf.iter, trf.reader, allCf, 2, 4);
    
    trf.closeReader();
  }
  
  public void test19() throws IOException {
    // test RFile metastore
    TestRFile trf = new TestRFile();
    
    trf.openWriter(false);
    
    trf.openWriter(false);
    trf.writer.startNewLocalityGroup("lg1", ncfs("cf1", "cf2"));
    trf.writer.append(nk("0000", "cf1", "doe,john", "", 4), nv("1123 West Left st"));
    trf.writer.append(nk("0002", "cf2", "doe,jane", "", 5), nv("1124 East Right st"));
    trf.writer.startNewLocalityGroup("lg2", ncfs("cf3", "cf4"));
    
    DataOutputStream dos = trf.writer.createMetaStore("count");
    
    dos.writeInt(2);
    dos.writeUTF("data1");
    dos.writeInt(1);
    dos.writeUTF("data2");
    dos.writeInt(1);
    
    dos.close();
    
    trf.closeWriter();
    
    trf.openReader();
    
    trf.iter.seek(new Range(), EMPTY_COL_FAMS, false);
    
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0000", "cf1", "doe,john", "", 4)));
    assertEquals(nv("1123 West Left st"), trf.iter.getTopValue());
    trf.iter.next();
    
    DataInputStream in = trf.reader.getMetaStore("count");
    
    assertEquals(2, in.readInt());
    assertEquals("data1", in.readUTF());
    assertEquals(1, in.readInt());
    assertEquals("data2", in.readUTF());
    assertEquals(1, in.readInt());
    
    in.close();
    
    assertTrue(trf.iter.hasTop());
    assertTrue(trf.iter.getTopKey().equals(nk("0002", "cf2", "doe,jane", "", 5)));
    assertEquals(nv("1124 East Right st"), trf.iter.getTopValue());
    trf.iter.next();
    assertFalse(trf.iter.hasTop());
    
    trf.closeReader();
  }
  
  public void testOldVersions() throws Exception {
    runVersionTest(3);
    runVersionTest(4);
  }
  
  private void runVersionTest(int version) throws IOException {
    InputStream in = this.getClass().getClassLoader().getResourceAsStream("org/apache/accumulo/core/file/rfile/ver_" + version + ".rf");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte buf[] = new byte[1024];
    int read;
    while ((read = in.read(buf)) > 0)
      baos.write(buf, 0, read);
    
    byte data[] = baos.toByteArray();
    SeekableByteArrayInputStream bais = new SeekableByteArrayInputStream(data);
    FSDataInputStream in2 = new FSDataInputStream(bais);
    CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(in2, data.length, CachedConfiguration.getInstance());
    Reader reader = new RFile.Reader(_cbr);
    checkIndex(reader);
    
    ColumnFamilySkippingIterator iter = new ColumnFamilySkippingIterator(reader);
    
    for (int start : new int[] {0, 10, 100, 998}) {
      for (int cf = 1; cf <= 4; cf++) {
        if (start == 0)
          iter.seek(new Range(), ncfs(nf("cf_", cf)), true);
        else
          iter.seek(new Range(nf("r_", start), null), ncfs(nf("cf_", cf)), true);
        
        for (int i = start; i < 1000; i++) {
          assertTrue(iter.hasTop());
          assertEquals(nk(nf("r_", i), nf("cf_", cf), nf("cq_", 0), "", 1000 - i), iter.getTopKey());
          assertEquals(nv(i + ""), iter.getTopValue());
          iter.next();
        }
        
        assertFalse(iter.hasTop());
      }
      
      if (start == 0)
        iter.seek(new Range(), ncfs(), false);
      else
        iter.seek(new Range(nf("r_", start), null), ncfs(), false);
      
      for (int i = start; i < 1000; i++) {
        for (int cf = 1; cf <= 4; cf++) {
          assertTrue(iter.hasTop());
          assertEquals(nk(nf("r_", i), nf("cf_", cf), nf("cq_", 0), "", 1000 - i), iter.getTopKey());
          assertEquals(nv(i + ""), iter.getTopValue());
          iter.next();
        }
      }
      
      assertFalse(iter.hasTop());
    }
    
    reader.close();
  }
}
