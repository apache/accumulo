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
package org.apache.accumulo.tserver.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Writer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MultiReaderTest {

  VolumeManager fs;
  TemporaryFolder root = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Before
  public void setUp() throws Exception {
    root.create();
    String path = root.getRoot().getAbsolutePath() + "/manyMaps";
    fs = VolumeManagerImpl.getLocal(path);
    Path root = new Path("file://" + path);
    fs.mkdirs(root);
    fs.create(new Path(root, "finished")).close();
    FileSystem ns = fs.getVolumeByPath(root).getFileSystem();

    Writer oddWriter = new Writer(ns.getConf(), ns.makeQualified(new Path(root, "odd")), Writer.keyClass(IntWritable.class),
        Writer.valueClass(BytesWritable.class));
    BytesWritable value = new BytesWritable("someValue".getBytes());
    for (int i = 1; i < 1000; i += 2) {
      oddWriter.append(new IntWritable(i), value);
    }
    oddWriter.close();

    Writer evenWriter = new Writer(ns.getConf(), ns.makeQualified(new Path(root, "even")), Writer.keyClass(IntWritable.class),
        Writer.valueClass(BytesWritable.class));
    for (int i = 0; i < 1000; i += 2) {
      if (i == 10)
        continue;
      evenWriter.append(new IntWritable(i), value);
    }
    evenWriter.close();
  }

  @After
  public void tearDown() throws Exception {
    root.create();
  }

  private void scan(MultiReader reader, int start) throws IOException {
    IntWritable key = new IntWritable();
    BytesWritable value = new BytesWritable();

    for (int i = start + 1; i < 1000; i++) {
      if (i == 10)
        continue;
      assertTrue(reader.next(key, value));
      assertEquals(i, key.get());
    }
  }

  private void scanOdd(MultiReader reader, int start) throws IOException {
    IntWritable key = new IntWritable();
    BytesWritable value = new BytesWritable();

    for (int i = start + 2; i < 1000; i += 2) {
      assertTrue(reader.next(key, value));
      assertEquals(i, key.get());
    }
  }

  @Test
  public void testMultiReader() throws IOException {
    Path manyMaps = new Path("file://" + root.getRoot().getAbsolutePath() + "/manyMaps");
    MultiReader reader = new MultiReader(fs, manyMaps);
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
    reader = new MultiReader(fs, manyMaps);
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

}
