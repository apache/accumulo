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
package org.apache.accumulo.server.tabletserver.log;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.tabletserver.log.MultiReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MultiReaderTest {
  
  Configuration conf = CachedConfiguration.getInstance();
  FileSystem fs;
  
  @Before
  public void setUp() throws Exception {
    // quiet log messages about compress.CodecPool
    Logger.getRootLogger().setLevel(Level.ERROR);
    fs = FileSystem.getLocal(conf);
    Path root = new Path("manyMaps");
    fs.mkdirs(root);
    fs.create(new Path(root, "finished")).close();
    Writer writer = new Writer(conf, fs, "manyMaps/odd", IntWritable.class, BytesWritable.class);
    BytesWritable value = new BytesWritable("someValue".getBytes());
    for (int i = 1; i < 1000; i += 2) {
      writer.append(new IntWritable(i), value);
    }
    writer.close();
    writer = new Writer(conf, fs, "manyMaps/even", IntWritable.class, BytesWritable.class);
    for (int i = 0; i < 1000; i += 2) {
      if (i == 10)
        continue;
      writer.append(new IntWritable(i), value);
    }
    writer.close();
  }
  
  @After
  public void tearDown() throws Exception {
    if (fs != null)
      fs.delete(new Path("manyMaps"), true);
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
    MultiReader reader = new MultiReader(fs, conf, "manyMaps");
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
    
    fs.delete(new Path("manyMaps/even"), true);
    reader = new MultiReader(fs, conf, "manyMaps");
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
