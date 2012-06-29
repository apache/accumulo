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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFileTest.SeekableByteArrayInputStream;
import org.apache.accumulo.core.iterators.Predicate;
import org.apache.accumulo.core.iterators.predicates.ColumnVisibilityPredicate;
import org.apache.accumulo.core.iterators.predicates.TimestampRangePredicate;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

public class AuthorizationFilterTest {
  
  @Test
  public void testRFileAuthorizationFiltering() throws Exception {
    Authorizations auths = new Authorizations("a", "b", "c");
    Predicate<Key,Value> columnVisibilityPredicate = new ColumnVisibilityPredicate(auths);
    int expected = 0;
    Random r = new Random();
    Configuration conf = new Configuration();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FSDataOutputStream dos = new FSDataOutputStream(baos, new FileSystem.Statistics("a"));
    CachableBlockFile.Writer _cbw = new CachableBlockFile.Writer(dos, "gz", conf);
    RFile.Writer writer = new RFile.Writer(_cbw, 1000, 1000);
    writer.startDefaultLocalityGroup();
    byte[] row = new byte[10];
    byte[] colFam = new byte[10];
    byte[] colQual = new byte[10];
    Value value = new Value(new byte[0]);
    TreeMap<Key,Value> inputBuffer = new TreeMap<Key,Value>();
    ColumnVisibility[] goodColVises = {new ColumnVisibility("a&b"), new ColumnVisibility("b&c"), new ColumnVisibility("a&c")};
    ColumnVisibility[] badColVises = {new ColumnVisibility("x"), new ColumnVisibility("y"), new ColumnVisibility("a&z")};
    for (ColumnVisibility colVis : goodColVises)
      for (int i = 0; i < 10; i++) {
        r.nextBytes(row);
        r.nextBytes(colFam);
        r.nextBytes(colQual);
        Key k = new Key(row, colFam, colQual, colVis.getExpression(), (long) i);
        if (columnVisibilityPredicate.evaluate(k, value))
          expected++;
        inputBuffer.put(k, value);
      }
    for (ColumnVisibility colVis : badColVises)
      for (int i = 0; i < 10000; i++) {
        r.nextBytes(row);
        r.nextBytes(colFam);
        r.nextBytes(colQual);
        Key k = new Key(row, colFam, colQual, colVis.getExpression(), (long) i);
        if (columnVisibilityPredicate.evaluate(k, value))
          expected++;
        inputBuffer.put(k, value);
      }
    for (Entry<Key,Value> e : inputBuffer.entrySet()) {
      writer.append(e.getKey(), e.getValue());
    }
    writer.close();
    
    // scan the RFile to bring back keys in a given timestamp range
    byte[] data = baos.toByteArray();
    
    ByteArrayInputStream bais = new SeekableByteArrayInputStream(data);
    FSDataInputStream in = new FSDataInputStream(bais);
    CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(in, data.length, conf);
    RFile.Reader reader = new RFile.Reader(_cbr);
    int count = 0;
    reader.applyFilter(columnVisibilityPredicate,true);
    reader.seek(new Range(), Collections.EMPTY_SET, false);
    while (reader.hasTop()) {
      count++;
      assertTrue(columnVisibilityPredicate.evaluate(reader.getTopKey(), reader.getTopValue()));
      reader.next();
    }
    assertEquals(expected, count);
  }
}
