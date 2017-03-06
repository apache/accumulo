/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.file.rfile.histogram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileScanner.NoopCache;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;

/**
 * Tests for a generic VisibilityHistogram.
 */
public class VisibilityHistogramTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static final Map<Key,Value> DATA = ImmutableMap.of(
      new Key("1", "", "", "PUBLIC"), new Value("a"),
      new Key("2", "", "", "PUBLIC"), new Value("b"),
      new Key("3", "", "", "PRIVATE"), new Value("c"));
  
  @Test
  public void testRFileHistogramSerialization() throws Exception {

    Configuration conf = new Configuration();
    conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
    FileSystem fs = FileSystem.get(conf);

    File f = folder.newFile();
    f.delete();
    String path = f.getAbsolutePath() + ".rf";
    System.out.println("Wrote " + path);
    try(RFileWriter writer = RFile.newWriter().to(path).withFileSystem(fs).build()) {
      writer.startDefaultLocalityGroup();
      writer.append(DATA.entrySet());
    }

    Map<Text,Long> expectedOutput = new HashMap<>();
    expectedOutput.put(new Text("PUBLIC"), Long.valueOf(2));
    expectedOutput.put(new Text("PRIVATE"), Long.valueOf(1));
    AccumuloConfiguration acuconf = AccumuloConfiguration.getDefaultConfiguration();
    try (org.apache.accumulo.core.file.rfile.RFile.Reader reader = new org.apache.accumulo.core.file.rfile.RFile.Reader(new CachableBlockFile.Reader(fs, new Path(path), conf, new NoopCache(), new NoopCache(), acuconf))) {
      VisibilityHistogram histogram = reader.getVisibilityHistogram();
      for (Entry<Text,Long> entry : histogram) {
        Long expectedValue = expectedOutput.remove(entry.getKey());
        assertEquals("Value for " + entry.getKey() + " was wrong", expectedValue, entry.getValue());
      }
    }
    assertTrue("Entries which should have existed in the histogram: " + expectedOutput, expectedOutput.isEmpty());
  }

  @Test public void testIdentitySerialization() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    HashMapVisibilityHistogram histogram = new HashMapVisibilityHistogram();
    for (Entry<Key,Value> entry : DATA.entrySet()) {
      histogram.increment(entry.getKey());
    }

    histogram.serialize(dos);
    byte[] bytes = baos.toByteArray();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
    HashMapVisibilityHistogram copy = new HashMapVisibilityHistogram();
    copy.deserialize(dis);

    HashMap<Text,Long> expected = new HashMap<>();
    HashMap<Text,Long> actual = new HashMap<>();
    for (Entry<Text,Long> entry : histogram) {
      expected.put(entry.getKey(), entry.getValue());
    }
    for (Entry<Text,Long> entry : copy) {
      actual.put(entry.getKey(), entry.getValue());
    }
    assertEquals(expected, actual);
  }
}
