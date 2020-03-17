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
package org.apache.accumulo.tserver.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.tserver.log.RecoveryLogReader.SortCheckIterator;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Writer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class RecoveryLogsReaderTest {

  private VolumeManager fs;
  private File workDir;

  @Rule
  public TemporaryFolder tempFolder =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Before
  public void setUp() throws Exception {
    workDir = tempFolder.newFolder();
    String path = workDir.getAbsolutePath();
    assertTrue(workDir.delete());
    fs = VolumeManagerImpl.getLocalForTesting(path);
    Path root = new Path("file://" + path);
    fs.mkdirs(root);
    fs.create(new Path(root, "finished")).close();
    FileSystem ns = fs.getFileSystemByPath(root);

    Writer oddWriter = new Writer(ns.getConf(), ns.makeQualified(new Path(root, "odd")),
        Writer.keyClass(IntWritable.class), Writer.valueClass(BytesWritable.class));
    BytesWritable value = new BytesWritable("someValue".getBytes());
    for (int i = 1; i < 1000; i += 2) {
      oddWriter.append(new IntWritable(i), value);
    }
    oddWriter.close();

    Writer evenWriter = new Writer(ns.getConf(), ns.makeQualified(new Path(root, "even")),
        Writer.keyClass(IntWritable.class), Writer.valueClass(BytesWritable.class));
    for (int i = 0; i < 1000; i += 2) {
      if (i == 10)
        continue;
      evenWriter.append(new IntWritable(i), value);
    }
    evenWriter.close();
  }

  @After
  public void tearDown() throws Exception {
    fs.close();
  }

  private void scan(RecoveryLogReader reader, int start) throws IOException {
    IntWritable key = new IntWritable();
    BytesWritable value = new BytesWritable();

    for (int i = start + 1; i < 1000; i++) {
      if (i == 10)
        continue;
      assertTrue(reader.next(key, value));
      assertEquals(i, key.get());
    }
  }

  private void scanOdd(RecoveryLogReader reader, int start) throws IOException {
    IntWritable key = new IntWritable();
    BytesWritable value = new BytesWritable();

    for (int i = start + 2; i < 1000; i += 2) {
      assertTrue(reader.next(key, value));
      assertEquals(i, key.get());
    }
  }

  @Test
  public void testMultiReader() throws IOException {
    Path manyMaps = new Path("file://" + workDir.getAbsolutePath());
    RecoveryLogReader reader = new RecoveryLogReader(fs, manyMaps);
    IntWritable key = new IntWritable();
    BytesWritable value = new BytesWritable();

    for (int i = 0; i < 1000; i++) {
      if (i == 10)
        continue;
      assertTrue(reader.next(key, value));
      assertEquals(i, key.get());
    }
    assertEquals(value.compareTo(new BytesWritable("someValue".getBytes())), 0);
    assertFalse(reader.next(key, value));

    key.set(500);
    assertTrue(reader.seek(key));
    scan(reader, 500);
    key.set(10);
    assertFalse(reader.seek(key));
    scan(reader, 10);
    key.set(1000);
    assertFalse(reader.seek(key));
    assertFalse(reader.next(key, value));
    key.set(-1);
    assertFalse(reader.seek(key));
    key.set(0);
    assertTrue(reader.next(key, value));
    assertEquals(0, key.get());
    reader.close();

    fs.deleteRecursively(new Path(manyMaps, "even"));
    reader = new RecoveryLogReader(fs, manyMaps);
    key.set(501);
    assertTrue(reader.seek(key));
    scanOdd(reader, 501);
    key.set(1000);
    assertFalse(reader.seek(key));
    assertFalse(reader.next(key, value));
    key.set(-1);
    assertFalse(reader.seek(key));
    key.set(1);
    assertTrue(reader.next(key, value));
    assertEquals(1, key.get());
    reader.close();

  }

  @Test(expected = IllegalStateException.class)
  public void testSortCheck() {

    List<Entry<LogFileKey,LogFileValue>> unsorted = new ArrayList<>();

    LogFileKey k1 = new LogFileKey();
    k1.event = LogEvents.MANY_MUTATIONS;
    k1.tabletId = 2;
    k1.seq = 55;

    LogFileKey k2 = new LogFileKey();
    k2.event = LogEvents.MANY_MUTATIONS;
    k2.tabletId = 9;
    k2.seq = 9;

    unsorted.add(new AbstractMap.SimpleEntry<>(k2, (LogFileValue) null));
    unsorted.add(new AbstractMap.SimpleEntry<>(k1, (LogFileValue) null));

    SortCheckIterator iter = new SortCheckIterator(unsorted.iterator());

    while (iter.hasNext()) {
      iter.next();
    }
  }

  /**
   * Test a failed marker doesn't cause issues. See Github issue
   * https://github.com/apache/accumulo/issues/961
   */
  @Test
  public void testFailed() throws Exception {
    Path manyMaps = new Path("file://" + workDir.getAbsolutePath());
    fs.create(new Path(manyMaps, SortedLogState.FAILED.getMarker())).close();

    RecoveryLogReader reader = new RecoveryLogReader(fs, manyMaps);
    IntWritable key = new IntWritable();
    BytesWritable value = new BytesWritable();

    for (int i = 0; i < 1000; i++) {
      if (i == 10)
        continue;
      assertTrue(reader.next(key, value));
      assertEquals(i, key.get());
    }
    reader.close();

    assertTrue(fs.delete(new Path(manyMaps, SortedLogState.FAILED.getMarker())));
  }

}
