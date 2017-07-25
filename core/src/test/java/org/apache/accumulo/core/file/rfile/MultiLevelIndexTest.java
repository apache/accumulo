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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.blockfile.ABlockWriter;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.BlockRead;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.BufferedWriter;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.IndexEntry;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.Reader;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.Reader.IndexIterator;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.Writer;
import org.apache.accumulo.core.file.rfile.RFileTest.SeekableByteArrayInputStream;
import org.apache.accumulo.core.file.streams.PositionedOutputs;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import junit.framework.TestCase;

public class MultiLevelIndexTest extends TestCase {

  public void test1() throws Exception {

    runTest(500, 1);
    runTest(500, 10);
    runTest(500, 100);
    runTest(500, 1000);
    runTest(500, 10000);

    runTest(1, 100);
  }

  private void runTest(int maxBlockSize, int num) throws IOException {
    AccumuloConfiguration aconf = DefaultConfiguration.getInstance();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream dos = new FSDataOutputStream(baos, new FileSystem.Statistics("a"));
    CachableBlockFile.Writer _cbw = new CachableBlockFile.Writer(PositionedOutputs.wrap(dos), "gz", CachedConfiguration.getInstance(), aconf);

    BufferedWriter mliw = new BufferedWriter(new Writer(_cbw, maxBlockSize));

    for (int i = 0; i < num; i++)
      mliw.add(new Key(String.format("%05d000", i)), i, 0, 0, 0);

    mliw.addLast(new Key(String.format("%05d000", num)), num, 0, 0, 0);

    ABlockWriter root = _cbw.prepareMetaBlock("root");
    mliw.close(root);
    root.close();

    _cbw.close();
    dos.close();
    baos.close();

    byte[] data = baos.toByteArray();
    SeekableByteArrayInputStream bais = new SeekableByteArrayInputStream(data);
    FSDataInputStream in = new FSDataInputStream(bais);
    CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(in, data.length, CachedConfiguration.getInstance(), aconf);

    Reader reader = new Reader(_cbr, RFile.RINDEX_VER_8);
    BlockRead rootIn = _cbr.getMetaBlock("root");
    reader.readFields(rootIn);
    rootIn.close();
    IndexIterator liter = reader.lookup(new Key("000000"));
    int count = 0;
    while (liter.hasNext()) {
      assertEquals(count, liter.nextIndex());
      assertEquals(count, liter.peek().getNumEntries());
      assertEquals(count, liter.next().getNumEntries());
      count++;
    }

    assertEquals(num + 1, count);

    while (liter.hasPrevious()) {
      count--;
      assertEquals(count, liter.previousIndex());
      assertEquals(count, liter.peekPrevious().getNumEntries());
      assertEquals(count, liter.previous().getNumEntries());
    }

    assertEquals(0, count);

    // go past the end
    liter = reader.lookup(new Key(String.format("%05d000", num + 1)));
    assertFalse(liter.hasNext());

    Random rand = new Random();
    for (int i = 0; i < 100; i++) {
      int k = rand.nextInt(num * 1000);
      int expected;
      if (k % 1000 == 0)
        expected = k / 1000; // end key is inclusive
      else
        expected = k / 1000 + 1;
      liter = reader.lookup(new Key(String.format("%08d", k)));
      IndexEntry ie = liter.next();
      assertEquals(expected, ie.getNumEntries());
    }

  }

}
