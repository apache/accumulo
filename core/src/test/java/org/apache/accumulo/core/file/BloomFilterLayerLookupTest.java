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
package org.apache.accumulo.core.file;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

public class BloomFilterLayerLookupTest {
  public static void main(String[] args) throws IOException {
    PrintStream out = System.out;
    
    Random r = new Random();
    
    HashSet<Integer> valsSet = new HashSet<Integer>();
    
    for (int i = 0; i < 100000; i++) {
      valsSet.add(r.nextInt(Integer.MAX_VALUE));
    }
    
    ArrayList<Integer> vals = new ArrayList<Integer>(valsSet);
    Collections.sort(vals);
    
    ConfigurationCopy acuconf = new ConfigurationCopy(AccumuloConfiguration.getDefaultConfiguration());
    acuconf.set(Property.TABLE_BLOOM_ENABLED, "true");
    acuconf.set(Property.TABLE_BLOOM_KEY_FUNCTOR, "accumulo.core.file.keyfunctor.ColumnFamilyFunctor");
    acuconf.set(Property.TABLE_FILE_TYPE, RFile.EXTENSION);
    acuconf.set(Property.TABLE_BLOOM_LOAD_THRESHOLD, "1");
    acuconf.set(Property.TSERV_BLOOM_LOAD_MAXCONCURRENT, "1");
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    
    String suffix = FileOperations.getNewFileExtension(acuconf);
    String fname = "/tmp/test." + suffix;
    FileSKVWriter bmfw = FileOperations.getInstance().openWriter(fname, fs, conf, acuconf);
    
    long t1 = System.currentTimeMillis();
    
    bmfw.startDefaultLocalityGroup();
    
    for (Integer i : vals) {
      String fi = String.format("%010d", i);
      bmfw.append(new org.apache.accumulo.core.data.Key(new Text("r" + fi), new Text("cf1")), new Value(("v" + fi).getBytes()));
      bmfw.append(new org.apache.accumulo.core.data.Key(new Text("r" + fi), new Text("cf2")), new Value(("v" + fi).getBytes()));
    }
    
    long t2 = System.currentTimeMillis();
    
    out.printf("write rate %6.2f%n", vals.size() / ((t2 - t1) / 1000.0));
    
    bmfw.close();
    
    t1 = System.currentTimeMillis();
    FileSKVIterator bmfr = FileOperations.getInstance().openReader(fname, false, fs, conf, acuconf);
    t2 = System.currentTimeMillis();
    out.println("Opened " + fname + " in " + (t2 - t1));
    
    t1 = System.currentTimeMillis();
    
    int hits = 0;
    for (int i = 0; i < 5000; i++) {
      int row = r.nextInt(Integer.MAX_VALUE);
      String fi = String.format("%010d", row);
      // bmfr.seek(new Range(new Text("r"+fi)));
      org.apache.accumulo.core.data.Key k1 = new org.apache.accumulo.core.data.Key(new Text("r" + fi), new Text("cf1"));
      bmfr.seek(new Range(k1, true, k1.followingKey(PartialKey.ROW_COLFAM), false), new ArrayList<ByteSequence>(), false);
      if (valsSet.contains(row)) {
        hits++;
        if (!bmfr.hasTop()) {
          out.println("ERROR " + row);
        }
      }
    }
    
    t2 = System.currentTimeMillis();
    
    out.printf("random lookup rate : %6.2f%n", 5000 / ((t2 - t1) / 1000.0));
    out.println("hits = " + hits);
    
    int count = 0;
    
    t1 = System.currentTimeMillis();
    
    for (Integer row : valsSet) {
      String fi = String.format("%010d", row);
      // bmfr.seek(new Range(new Text("r"+fi)));
      
      org.apache.accumulo.core.data.Key k1 = new org.apache.accumulo.core.data.Key(new Text("r" + fi), new Text("cf1"));
      bmfr.seek(new Range(k1, true, k1.followingKey(PartialKey.ROW_COLFAM), false), new ArrayList<ByteSequence>(), false);
      
      if (!bmfr.hasTop()) {
        out.println("ERROR 2 " + row);
      }
      
      count++;
      
      if (count >= 500) {
        break;
      }
    }
    
    t2 = System.currentTimeMillis();
    
    out.printf("existant lookup rate %6.2f%n", 500 / ((t2 - t1) / 1000.0));
    out.println("expected hits 500.  Receive hits: " + count);
    bmfr.close();
  }

}
