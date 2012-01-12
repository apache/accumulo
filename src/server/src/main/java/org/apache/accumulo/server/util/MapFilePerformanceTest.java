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
package org.apache.accumulo.server.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * @deprecated since 1.4
 */
public class MapFilePerformanceTest {
  
  public static String[] createMapFiles(String input, String output, int blocksize, int mapFiles) throws IOException {
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    
    SequenceFile.Reader in = new SequenceFile.Reader(fs, new Path(input + "/" + MapFile.DATA_FILE_NAME), conf);
    
    boolean someFilesExist = false;
    
    MapFile.Writer out[] = new MapFile.Writer[mapFiles];
    for (int i = 0; i < out.length; i++) {
      if (!fs.exists(new Path(output + "_" + i + "_" + mapFiles))) {
        out[i] = new MapFile.Writer(conf, fs, output + "_" + i + "_" + mapFiles, Key.class, Value.class, SequenceFile.CompressionType.RECORD);
      } else {
        someFilesExist = true;
      }
    }
    
    Key key = new Key();
    Value value = new Value();
    
    Random r = new Random();
    
    if (someFilesExist) {
      System.out.println("NOT Creating " + mapFiles + " map files using a compression block size of " + blocksize + " some files exist");
    } else {
      while (in.next(key, value)) {
        int i = r.nextInt(mapFiles);
        out[i].append(key, value);
      }
    }
    
    String names[] = new String[mapFiles];
    
    in.close();
    for (int i = 0; i < out.length; i++) {
      if (out[i] != null) {
        out[i].close();
      }
      names[i] = output + "_" + i + "_" + mapFiles;
    }
    
    return names;
  }
  
  public static void selectRandomKeys(String input, double percentage, ArrayList<Key> keys) throws IOException {
    
    System.out.println("Selecting random keys ...");
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    
    Random r = new Random();
    
    SequenceFile.Reader in = new SequenceFile.Reader(fs, new Path(input + "/" + MapFile.DATA_FILE_NAME), conf);
    
    Key key = new Key();
    
    while (in.next(key)) {
      if (r.nextDouble() < percentage)
        keys.add(new Key(key));
    }
    
    in.close();
    
    Collections.shuffle(keys);
    
    System.out.println("Selected " + keys.size() + " random keys.");
  }
  
  public static void runTest(String testName, String mapFiles[], ArrayList<Key> queries) throws IOException {
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    
    if (mapFiles.length == 1) {
      MapFile.Reader mr = new MapFile.Reader(fs, mapFiles[0], conf);
      
      Value value = new Value();
      
      long t1 = System.currentTimeMillis();
      int count = 0;
      int misses = 0;
      for (Key key : queries) {
        Key key2 = (Key) mr.getClosest(key, value);
        if (key2.compareTo(key) != 0) {
          misses++;
        }
        count++;
      }
      
      long t2 = System.currentTimeMillis();
      
      mr.close();
      
      double secs = (t2 - t1) / 1000.0;
      double queriesPerSec = count / (secs);
      
      System.out.printf("DIRECT %40s q/s = %8.2f s = %8.2f m = %,d\n", testName, queriesPerSec, secs, misses);
    }
    
    MyMapFile.Reader readers[] = new MyMapFile.Reader[mapFiles.length];
    for (int i = 0; i < mapFiles.length; i++) {
      readers[i] = new MyMapFile.Reader(fs, mapFiles[i], conf);
    }
    
    List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<SortedKeyValueIterator<Key,Value>>(readers.length);
    for (int i = 0; i < readers.length; i++) {
      iters.add(readers[i]);
    }
    
    MultiIterator mmfi = new MultiIterator(iters, new KeyExtent(new Text(""), null, null));
    mmfi.seek(new Range(new Key(), null), LocalityGroupUtil.EMPTY_CF_SET, false);
    
    long t1 = System.currentTimeMillis();
    int count = 0;
    int misses = 0;
    for (Key key : queries) {
      mmfi.seek(new Range(key, null), LocalityGroupUtil.EMPTY_CF_SET, false);
      if (mmfi.getTopKey().compareTo(key) != 0)
        misses++;
      count++;
    }
    
    long t2 = System.currentTimeMillis();
    
    double secs = (t2 - t1) / 1000.0;
    double queriesPerSec = count / (secs);
    
    System.out.printf("MMFI   %40s q/s = %8.2f s = %8.2f m = %,d\n", testName, queriesPerSec, secs, misses);
    
    for (int i = 0; i < mapFiles.length; i++) {
      readers[i].close();
    }
  }
  
  public static void main(final String[] args) throws IOException, InterruptedException {
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    
    final ArrayList<Key> keys = new ArrayList<Key>();
    
    int blocksizes[] = new int[] {10000};
    int numMapFiles[] = new int[] {1, 2, 3, 5, 7};
    
    ExecutorService tp = Executors.newFixedThreadPool(10);
    
    Runnable selectKeysTask = new Runnable() {
      
      public void run() {
        try {
          selectRandomKeys(args[0], .002, keys);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      
    };
    
    tp.submit(selectKeysTask);
    
    final Map<Integer,Map<Integer,String[]>> tests = new HashMap<Integer,Map<Integer,String[]>>();
    
    for (final int num : numMapFiles) {
      for (final int blocksize : blocksizes) {
        
        Runnable r = new Runnable() {
          public void run() {
            System.out.println("Thread " + Thread.currentThread().getName() + " creating map files blocksize = " + blocksize + " num = " + num);
            String[] filenames;
            try {
              filenames = createMapFiles(args[0], args[1] + "/" + MyMapFile.EXTENSION + "_" + blocksize, blocksize, num);
              
              synchronized (tests) {
                Map<Integer,String[]> map = tests.get(num);
                if (map == null) {
                  map = new HashMap<Integer,String[]>();
                  tests.put(num, map);
                }
                
                map.put(blocksize, filenames);
              }
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
            System.out.println("Thread " + Thread.currentThread().getName() + " finished creating map files");
            
          }
        };
        
        tp.execute(r);
      }
    }
    
    tp.shutdown();
    while (!tp.isTerminated()) {
      tp.awaitTermination(1, TimeUnit.DAYS);
    }
    
    for (int num : numMapFiles) {
      for (int blocksize : blocksizes) {
        String[] filenames = tests.get(num).get(blocksize);
        
        long len = 0;
        for (String filename : filenames) {
          len += fs.getFileStatus(new Path(filename + "/" + MapFile.DATA_FILE_NAME)).getLen();
        }
        runTest(String.format("bs = %,12d fs = %,12d nmf = %d ", blocksize, len, num), filenames, keys);
        runTest(String.format("bs = %,12d fs = %,12d nmf = %d ", blocksize, len, num), filenames, keys);
      }
    }
  }
  
}
