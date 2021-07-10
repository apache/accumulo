/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.file.rfile;

import static org.apache.accumulo.core.spi.crypto.CryptoEnvironment.Scope.TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.crypto.CryptoServiceFactory.ClassloaderType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.CachableBuilder;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.BufferedWriter;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.IndexEntry;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.Reader;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.Reader.IndexIterator;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.Writer;
import org.apache.accumulo.core.file.rfile.RFileTest.SeekableByteArrayInputStream;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

public class MultiLevelIndexTest {
  private Configuration hadoopConf = new Configuration();

  @Test
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
    BCFile.Writer _cbw = new BCFile.Writer(dos, null, "gz", hadoopConf,
        CryptoServiceFactory.newInstance(aconf, ClassloaderType.JAVA, TABLE));

    BufferedWriter mliw = new BufferedWriter(new Writer(_cbw, maxBlockSize));

    for (int i = 0; i < num; i++)
      mliw.add(new Key(String.format("%05d000", i)), i, 0, 0, 0);

    mliw.addLast(new Key(String.format("%05d000", num)), num, 0, 0, 0);

    BCFile.Writer.BlockAppender root = _cbw.prepareMetaBlock("root");
    mliw.close(root);
    root.close();

    _cbw.close();
    dos.close();
    baos.close();

    byte[] data = baos.toByteArray();
    SeekableByteArrayInputStream bais = new SeekableByteArrayInputStream(data);
    FSDataInputStream in = new FSDataInputStream(bais);
    CachableBuilder cb = new CachableBuilder().input(in).length(data.length).conf(hadoopConf)
        .cryptoService(CryptoServiceFactory.newInstance(aconf, ClassloaderType.JAVA, TABLE));
    CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(cb);

    Reader reader = new Reader(_cbr, RFile.RINDEX_VER_8);
    CachableBlockFile.CachedBlockRead rootIn = _cbr.getMetaBlock("root");
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

    Random rand = new SecureRandom();
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
