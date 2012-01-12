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
package org.apache.accumulo.server.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.map.MapFileUtil;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

/**
 * @deprecated since 1.4
 */
public class MMFITest {
  private static MyMapFile.Reader[] mapfiles = null;
  private static Text startRow;
  private static Text prevEndRow;
  private static Text endRow;
  
  public static void main(String[] args) {
    try {
      mapfiles = new MyMapFile.Reader[args.length - 3];
      
      Configuration conf = CachedConfiguration.getInstance();
      FileSystem fs = FileSystem.get(conf);
      
      startRow = new Text(String.format("row_%010d", Integer.parseInt(args[0])));
      prevEndRow = new Text(String.format("row_%010d", Integer.parseInt(args[1])));
      endRow = new Text(String.format("row_%010d", Integer.parseInt(args[2])));
      
      for (int i = 3; i < args.length; i++)
        mapfiles[i - 3] = MapFileUtil.openMapFile((AccumuloConfiguration) null, fs, args[i], conf);
    } catch (IOException e) {
      for (MyMapFile.Reader r : mapfiles)
        try {
          if (r != null)
            r.close();
        } catch (IOException e1) {
          // close the rest anyway
        }
      throw new RuntimeException(e);
    }
    
    try {
      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<SortedKeyValueIterator<Key,Value>>(mapfiles.length);
      for (int i = 0; i < mapfiles.length; i++)
        iters.add(mapfiles[i]);
      
      MultiIterator mmfi = new MultiIterator(iters, new KeyExtent(new Text(""), endRow, prevEndRow));
      mmfi.seek(new Range(new Key(startRow), null), LocalityGroupUtil.EMPTY_CF_SET, false);
      
      int count = 0;
      
      long t1 = System.currentTimeMillis();
      
      Key lastKey = new Key();
      
      while (mmfi.hasTop()) {
        Key key = mmfi.getTopKey();
        
        if (lastKey.compareTo(key) > 0) {
          String msg = "Not sorted : " + lastKey + " " + key;
          System.err.println(msg);
          throw new RuntimeException(msg);
        }
        
        lastKey.set(key);
        
        System.out.println(" " + key);
        
        mmfi.next();
        count++;
      }
      
      long t2 = System.currentTimeMillis();
      
      double time = (t2 - t1) / 1000.0;
      System.out.printf("count : %,d   time : %6.2f  rate : %,6.2f\n", count, time, count / time);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      for (MyMapFile.Reader r : mapfiles)
        try {
          if (r != null)
            r.close();
        } catch (IOException e) {
          // close the rest anyway
        }
    }
  }
}
