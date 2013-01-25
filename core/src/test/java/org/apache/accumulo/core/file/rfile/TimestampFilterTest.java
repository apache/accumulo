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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFileTest.SeekableByteArrayInputStream;
import org.apache.accumulo.core.iterators.Predicate;
import org.apache.accumulo.core.iterators.predicates.TimestampRangePredicate;
import org.apache.accumulo.core.iterators.system.GenericFilterer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

public class TimestampFilterTest {
  
  @SuppressWarnings("unchecked")
  @Test
  public void testRFileTimestampFiltering() throws Exception {
    Predicate<Key,Value> timeRange = new TimestampRangePredicate(73, 117);
    int expected = 0;
    Random r = new Random();
    Configuration conf = new Configuration();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream dos = new FSDataOutputStream(baos, new FileSystem.Statistics("a"));
    CachableBlockFile.Writer _cbw = new CachableBlockFile.Writer(dos, "gz", conf);
    RFile.Writer writer = new RFile.Writer(_cbw, 1000, 1000);
    writer.startDefaultLocalityGroup();
    byte [] row = new byte[10];
    byte [] colFam = new byte[10];
    byte [] colQual = new byte[10];
    Value value = new Value(new byte[0]);
    byte [] colVis = new byte[0];
    TreeMap<Key,Value> inputBuffer = new TreeMap<Key,Value>();
    for(int i = 0; i < 100000; i++)
    {
      r.nextBytes(row);
      r.nextBytes(colFam);
      r.nextBytes(colQual);
      Key k = new Key(row,colFam,colQual,colVis,(long)i);
      if(timeRange.evaluate(k, value))
        expected++;
      inputBuffer.put(k, value);
    }
    for(Entry<Key,Value> e:inputBuffer.entrySet())
    {
      writer.append(e.getKey(), e.getValue());
    }
    writer.close();

    // scan the RFile to bring back keys in a given timestamp range
    byte[] data = baos.toByteArray();
    ByteArrayInputStream bais = new SeekableByteArrayInputStream(data);
    FSDataInputStream in = new FSDataInputStream(bais);
    CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(in, data.length, conf);
    RFile.Reader reader = new RFile.Reader(_cbr);
    GenericFilterer filterer = new GenericFilterer(reader);
    int count = 0;
    filterer.applyFilter(timeRange,true);
    filterer.seek(new Range(), Collections.EMPTY_SET, false);
    while(filterer.hasTop())
    {
      count++;
      assertTrue(timeRange.evaluate(filterer.getTopKey(),filterer.getTopValue()));
      filterer.next();
    }
    assertEquals(expected, count);
  }
  
}
