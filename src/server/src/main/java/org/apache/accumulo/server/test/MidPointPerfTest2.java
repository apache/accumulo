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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.file.map.MySequenceFile;
import org.apache.accumulo.core.file.map.MySequenceFile.Reader;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @deprecated since 1.4
 */
class MultipleIndexIterator2 {
  
  private MySequenceFile.Reader[] readers;
  private boolean[] hasNextKey;
  private Key[] nextKey;
  private int currentMin = 0;
  
  int findMinimum() {
    int minIndex = -1;
    for (int i = 0; i < nextKey.length; i++) {
      if (!hasNextKey[i]) {
        continue;
      }
      
      if (minIndex == -1) {
        minIndex = i;
      } else if (nextKey[i].compareTo(nextKey[minIndex]) < 0) {
        minIndex = i;
      }
    }
    
    return minIndex;
  }
  
  MultipleIndexIterator2(Configuration conf, FileSystem fs, List<Path> paths) throws IOException, InstantiationException, IllegalAccessException {
    readers = new MySequenceFile.Reader[paths.size()];
    
    int ri = 0;
    for (Path path : paths) {
      MySequenceFile.Reader index = new MySequenceFile.Reader(fs, path, conf);
      readers[ri++] = index;
    }
    
    hasNextKey = new boolean[readers.length];
    nextKey = new Key[readers.length];
    for (int i = 0; i < readers.length; i++) {
      nextKey[i] = (Key) readers[i].getKeyClass().newInstance();
      hasNextKey[i] = readers[i].next(nextKey[i]);
    }
    
    currentMin = findMinimum();
  }
  
  boolean hasNext() {
    return currentMin >= 0;
  }
  
  WritableComparable<?> next() {
    if (currentMin < 0) {
      throw new RuntimeException("There is no next");
    }
    
    WritableComparable<?> ret = nextKey[currentMin];
    
    try {
      nextKey[currentMin] = (Key) readers[currentMin].getKeyClass().newInstance();
      hasNextKey[currentMin] = readers[currentMin].next(nextKey[currentMin]);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    currentMin = findMinimum();
    
    return ret;
  }
  
  public void close() {
    currentMin = -1;
    
    for (Reader reader : readers) {
      try {
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}

/**
 * @deprecated since 1.4
 */
public class MidPointPerfTest2 {
  private static String dir;
  private static int numFiles;
  private static int numEntries;
  private static int min;
  private static int max;
  private static int maxFiles;
  private static String tmpDir;
  private static boolean createTestData = false;
  
  public static void main(String[] args) {
    
    for (String string : args) {
      System.out.println("arg : " + string);
    }
    
    if (args[0].equals("-ctd")) {
      dir = args[1];
      numFiles = Integer.parseInt(args[2]);
      numEntries = Integer.parseInt(args[3]);
      min = Integer.parseInt(args[4]);
      max = Integer.parseInt(args[5]);
      
      createTestData = true;
    } else {
      dir = args[0];
      maxFiles = Integer.parseInt(args[1]);
      tmpDir = args[2];
    }
    
    try {
      if (createTestData) {
        Random r = new Random();
        createTestData(dir, numFiles, numEntries, min, max, r);
      } else {
        timeIterate(dir, maxFiles, tmpDir);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private static void timeIterate(String dir, int maxFiles, String tmpDir) throws Exception {
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    
    FileStatus[] files = fs.globStatus(new Path(dir + "/*/index"));
    ArrayList<Path> paths = new ArrayList<Path>(files.length);
    
    for (FileStatus fileStatus : files) {
      paths.add(fileStatus.getPath());
    }
    
    long t1 = System.currentTimeMillis();
    ArrayList<Path> rpaths = reduceFiles(conf, fs, paths, maxFiles, tmpDir, 0);
    long t2 = System.currentTimeMillis();
    
    MultipleIndexIterator2 mii = new MultipleIndexIterator2(conf, fs, rpaths);
    
    int count = 0;
    while (mii.hasNext()) {
      mii.next();
      count++;
    }
    
    long t3 = System.currentTimeMillis();
    
    System.out.printf("reduce time  : %6.2f secs \n", (t2 - t1) / 1000.0);
    System.out.printf("iterate time : %6.2f secs \n", (t3 - t2) / 1000.0);
    System.out.printf("total time   : %6.2f secs \n", (t3 - t1) / 1000.0);
    
    System.out.println("count " + count);
  }
  
  private static ArrayList<Path> reduceFiles(Configuration conf, FileSystem fs, ArrayList<Path> paths, int maxFiles, String tmpDir, int pass)
      throws IOException, InstantiationException, IllegalAccessException {
    if (paths.size() <= maxFiles) {
      return paths;
    }
    
    String newDir = String.format("%s/pass_%04d", tmpDir, pass);
    fs.mkdirs(new Path(newDir));
    
    int start = 0;
    
    ArrayList<Path> outFiles = new ArrayList<Path>();
    
    int count = 0;
    
    while (start < paths.size()) {
      int end = Math.min(maxFiles + start, paths.size());
      List<Path> inFiles = paths.subList(start, end);
      
      start = end;
      
      Path outFile = new Path(String.format("%s/index_%04d", newDir, count++));
      outFiles.add(outFile);
      
      long t1 = System.currentTimeMillis();
      
      MySequenceFile.Writer writer = MySequenceFile.createWriter(fs, conf, outFile, Key.class, LongWritable.class, MySequenceFile.CompressionType.BLOCK);
      MultipleIndexIterator2 mii = new MultipleIndexIterator2(conf, fs, inFiles);
      
      while (mii.hasNext()) {
        writer.append(mii.next(), new LongWritable(0));
      }
      
      mii.close();
      writer.close();
      
      long t2 = System.currentTimeMillis();
      
      System.out.printf("out : %s  num in : %d   time : %6.2f secs\n", outFile, inFiles.size(), (t2 - t1) / 1000.0);
    }
    
    return reduceFiles(conf, fs, outFiles, maxFiles, tmpDir, pass + 1);
  }
  
  static class CompareKeys implements Comparator<Key> {
    public CompareKeys() {}
    
    public boolean equals(Key a, Key b) {
      return a.equals(b);
    }
    
    public int compare(Key a, Key b) {
      return a.compareTo(b);
    }
  }
  
  private static void createTestData(String dir, int numFiles, int numEntries, int min, int max, Random r) throws Exception {
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    
    for (int i = 0; i < numFiles; i++) {
      String newDir = String.format("%s/" + MyMapFile.EXTENSION + "_%06d", dir, i);
      fs.mkdirs(new Path(newDir));
      
      List<Key> keys = new ArrayList<Key>();
      
      for (int j = 0; j < numEntries; j++) {
        String row = String.format("row_%010d", r.nextInt() % (max - min) + min);
        Key key1 = new Key(new Text(row), new Text(String.format("cf_%03d", r.nextInt() % 100)), new Text(String.format("cf_%05d", r.nextInt() % 10000)));
        keys.add(key1);
      }
      
      Collections.sort(keys, new CompareKeys());
      
      MySequenceFile.Writer writer = MySequenceFile.createWriter(fs, conf, new Path(newDir + "/index"), Key.class, LongWritable.class,
          MySequenceFile.CompressionType.BLOCK);
      
      System.out.println(new Path(newDir + "/index"));
      
      for (Key key : keys) {
        writer.append(key, new LongWritable(0));
      }
      
      writer.close();
    }
  }
  
}
