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
package org.apache.accumulo.core.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.file.map.MySequenceFile;
import org.apache.accumulo.core.file.map.MySequenceFile.CompressionType;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * @deprecated since 1.4
 */
public class MapFileTest extends TestCase {
  private static final Logger log = Logger.getLogger(MapFileTest.class);
  
  public void testMapFileSeek() {
    try {
      Configuration conf = CachedConfiguration.getInstance();
      FileSystem fs = FileSystem.get(conf);
      conf.setInt("io.seqfile.compress.blocksize", 100);
      
      HashMap<Key,Value> kvMap = new HashMap<Key,Value>();
      
      /*****************************
       * write out the test map file
       */
      MyMapFile.Writer mfw = new MyMapFile.Writer(conf, fs, "/tmp/testMapFileIndexingMap", Key.class, Value.class, CompressionType.BLOCK);
      Value value = new Value(new byte[10]);
      for (int i = 0; i < 10; i++) {
        Text row = new Text(String.format("%08d", i));
        for (int j = 0; j < 10; j++) {
          Text cf = new Text(String.format("%08d", j));
          for (int k = 0; k < 10; k++) {
            Text cq = new Text(String.format("%08d", k));
            Key key = new Key(row, cf, cq);
            mfw.append(key, value);
            kvMap.put(key, value);
          }
        }
      }
      mfw.close();
      
      MyMapFile.Reader mfr = new MyMapFile.Reader(fs, "/tmp/testMapFileIndexingMap", conf);
      value = new Value();
      
      // make sure we can seek to each of the keys
      for (Entry<Key,Value> e : kvMap.entrySet()) {
        Range range = new Range(e.getKey(), null);
        mfr.seek(range, LocalityGroupUtil.EMPTY_CF_SET, false);
        assertTrue(mfr.hasTop());
        Key k = mfr.getTopKey();
        assertTrue(k.equals(e.getKey()));
      }
      mfr.close();
      
      fs.delete(new Path("/tmp/testMapFileIndexingMap"), true);
    } catch (IOException e) {
      log.error(e, e);
      assertTrue(false);
    }
    
  }
  
  public void testMapFileIndexing() {
    try {
      Configuration conf = CachedConfiguration.getInstance();
      FileSystem fs = FileSystem.get(conf);
      conf.setInt("io.seqfile.compress.blocksize", 2000);
      
      /*****************************
       * write out the test map file
       */
      MyMapFile.Writer mfw = new MyMapFile.Writer(conf, fs, "/tmp/testMapFileIndexingMap", Text.class, BytesWritable.class, CompressionType.BLOCK);
      Text key = new Text();
      BytesWritable value;
      Random r = new Random();
      byte[] bytes = new byte[1024];
      ArrayList<String> keys = new ArrayList<String>();
      ArrayList<String> badKeys = new ArrayList<String>();
      for (int i = 0; i < 1000; i++) {
        String keyString = Integer.toString(i + 1000000);
        keys.add(keyString);
        badKeys.add(keyString + "_");
        key.set(keyString);
        r.nextBytes(bytes);
        bytes[0] = (byte) keyString.charAt(0);
        bytes[1] = (byte) keyString.charAt(1);
        bytes[2] = (byte) keyString.charAt(2);
        bytes[3] = (byte) keyString.charAt(3);
        bytes[4] = (byte) keyString.charAt(4);
        bytes[5] = (byte) keyString.charAt(5);
        bytes[6] = (byte) keyString.charAt(6);
        value = new BytesWritable(bytes);
        mfw.append(key, value);
      }
      mfw.close();
      
      MyMapFile.Reader mfr = new MyMapFile.Reader(fs, "/tmp/testMapFileIndexingMap", conf);
      value = new BytesWritable();
      // test edge cases
      key.set(keys.get(0));
      assertTrue(mfr.seek(key));
      key.set(keys.get(keys.size() - 1));
      assertTrue(mfr.seek(key));
      key.set("");
      assertFalse(mfr.seek(key));
      key.set(keys.get(keys.size() - 1) + "_");
      assertFalse(mfr.seek(key));
      
      // test interaction with nextKey
      key.set(keys.get(17));
      assertTrue(mfr.seek(key));
      assertTrue(mfr.next(key, value));
      assertTrue(mfr.seek(key, key));
      
      // test seeking before the beginning of the file
      key.set("");
      Text closestKey = (Text) mfr.getClosest(key, value, false, null);
      assertTrue(closestKey.toString().equals(keys.get(0)));
      assertTrue(value.getBytes()[6] == (byte) (closestKey.toString().charAt(6)));
      closestKey = (Text) mfr.getClosest(key, value, false, closestKey);
      assertTrue(closestKey.toString().equals(keys.get(0)));
      assertTrue(value.getBytes()[6] == (byte) (closestKey.toString().charAt(6)));
      closestKey = (Text) mfr.getClosest(key, value, false, closestKey);
      assertTrue(closestKey.toString().equals(keys.get(0)));
      assertTrue(value.getBytes()[6] == (byte) (closestKey.toString().charAt(6)));
      
      // test seeking after the end of the file
      key.set(keys.get(keys.size() - 1));
      closestKey = (Text) mfr.getClosest(key, value, false, null);
      assertTrue(keys.get(keys.size() - 1).equals(closestKey.toString()));
      key.set("z");
      closestKey = (Text) mfr.getClosest(key, value, false, closestKey);
      assertTrue(closestKey == null);
      key.set("zz");
      closestKey = (Text) mfr.getClosest(key, value, false, closestKey);
      assertTrue(closestKey == null);
      
      key.set(keys.get(keys.size() - 1));
      closestKey = (Text) mfr.getClosest(key, value, false, null);
      assertFalse(mfr.next(closestKey, value));
      key.set("z");
      closestKey = (Text) mfr.getClosest(key, value, false, closestKey);
      assertTrue(closestKey == null);
      
      // test sequential reads
      for (int sample = 0; sample < 1000; sample++) {
        key.set(keys.get(sample));
        assertTrue(mfr.seek(key));
      }
      
      // test sequential misses
      for (int sample = 0; sample < 1000; sample++) {
        key.set(badKeys.get(sample));
        assertFalse(mfr.seek(key));
      }
      
      // test backwards reads
      for (int sample = 0; sample < 1000; sample++) {
        key.set(keys.get(keys.size() - 1 - sample));
        assertTrue(mfr.seek(key));
      }
      
      // test backwards misses
      for (int sample = 0; sample < 1000; sample++) {
        key.set(badKeys.get(badKeys.size() - 1 - sample));
        assertFalse(mfr.seek(key));
      }
      
      // test interleaved reads
      for (int sample = 0; sample < 1000; sample++) {
        key.set(keys.get(sample));
        assertTrue(mfr.seek(key));
        key.set(badKeys.get(sample));
        assertFalse(mfr.seek(key));
      }
      
      // test random reads
      Collections.shuffle(keys);
      Collections.shuffle(badKeys);
      for (int sample = 0; sample < 1000; sample++) {
        key.set(keys.get(sample));
        boolean seekGood = mfr.seek(key);
        if (!seekGood) {
          log.info("Key: " + keys.get(sample));
          if (sample != 0) {
            log.info("Last key: " + keys.get(sample - 1));
          }
        }
        assertTrue(seekGood);
        key.set(badKeys.get(sample));
        assertTrue(!mfr.seek(key));
      }
      
      fs.delete(new Path("/tmp/testMapFileIndexingMap"), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void testMapFileFix() {
    try {
      Configuration conf = CachedConfiguration.getInstance();
      FileSystem fs = FileSystem.get(conf);
      conf.setInt("io.seqfile.compress.blocksize", 4000);
      
      for (CompressionType compressionType : CompressionType.values()) {
        /*****************************
         * write out the test map file
         */
        MyMapFile.Writer mfw = new MyMapFile.Writer(conf, fs, "/tmp/testMapFileIndexingMap", Text.class, BytesWritable.class, compressionType);
        BytesWritable value;
        Random r = new Random();
        byte[] bytes = new byte[1024];
        for (int i = 0; i < 1000; i++) {
          String keyString = Integer.toString(i + 1000000);
          Text key = new Text(keyString);
          r.nextBytes(bytes);
          value = new BytesWritable(bytes);
          mfw.append(key, value);
        }
        mfw.close();
        
        /************************************
         * move the index file
         */
        fs.rename(new Path("/tmp/testMapFileIndexingMap/index"), new Path("/tmp/testMapFileIndexingMap/oldIndex"));
        
        /************************************
         * recreate the index
         */
        MyMapFile.fix(fs, new Path("/tmp/testMapFileIndexingMap"), Text.class, BytesWritable.class, false, conf);
        
        /************************************
         * compare old and new indices
         */
        MySequenceFile.Reader oldIndexReader = new MySequenceFile.Reader(fs, new Path("/tmp/testMapFileIndexingMap/oldIndex"), conf);
        MySequenceFile.Reader newIndexReader = new MySequenceFile.Reader(fs, new Path("/tmp/testMapFileIndexingMap/index"), conf);
        
        Text oldKey = new Text();
        Text newKey = new Text();
        LongWritable oldValue = new LongWritable();
        LongWritable newValue = new LongWritable();
        while (true) {
          boolean moreKeys = false;
          // check for the same number of records
          assertTrue((moreKeys = oldIndexReader.next(oldKey, oldValue)) == newIndexReader.next(newKey, newValue));
          if (!moreKeys)
            break;
          assertTrue(oldKey.compareTo(newKey) == 0);
          assertTrue(oldValue.compareTo(newValue) == 0);
        }
        oldIndexReader.close();
        newIndexReader.close();
        
        fs.delete(new Path("/tmp/testMapFileIndexingMap"), true);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public static void main(String[] args) {
    try {
      Configuration conf = CachedConfiguration.getInstance();
      FileSystem fs = FileSystem.get(conf);
      MyMapFile.Writer mfw = new MyMapFile.Writer(conf, fs, "/tmp/testMapFileIndexingMap", Text.class, BytesWritable.class, CompressionType.BLOCK);
      Text key = new Text();
      BytesWritable value;
      Random r = new Random();
      byte[] bytes = new byte[1024];
      ArrayList<String> keys = new ArrayList<String>();
      ArrayList<String> badKeys = new ArrayList<String>();
      for (int i = 0; i < 100000; i++) {
        String keyString = Integer.toString(i + 1000000);
        keys.add(keyString);
        badKeys.add(keyString + "_");
        key.set(keyString);
        r.nextBytes(bytes);
        value = new BytesWritable(bytes);
        mfw.append(key, value);
      }
      mfw.close();
      
      MyMapFile.Reader mfr = new MyMapFile.Reader(fs, "/tmp/testMapFileIndexingMap", conf);
      
      long t1 = System.currentTimeMillis();
      
      value = new BytesWritable();
      key.set(keys.get(0));
      mfr.seek(key);
      while (mfr.next(key, value))
        continue;
      
      long t2 = System.currentTimeMillis();
      
      log.info("Scan time: " + (t2 - t1));
      
      t1 = System.currentTimeMillis();
      Text key2 = new Text();
      for (int i = 0; i < 100000; i += 1000) {
        key.set(keys.get(i));
        key2 = (Text) mfr.getClosest(key, value, false, key2);
      }
      t2 = System.currentTimeMillis();
      
      log.info("Seek time: " + (t2 - t1));
      
      t1 = System.currentTimeMillis();
      
      value = new BytesWritable();
      key.set(keys.get(0));
      mfr.seek(key);
      while (mfr.next(key, value))
        continue;
      
      t2 = System.currentTimeMillis();
      log.info("Scan time: " + (t2 - t1));
      t1 = System.currentTimeMillis();
      key2 = new Text();
      for (int i = 0; i < 100000; i += 1000) {
        key.set(keys.get(i));
        key2 = (Text) mfr.getClosest(key, value, false, key2);
      }
      t2 = System.currentTimeMillis();
      
      log.info("Seek time: " + (t2 - t1));
      
      fs.delete(new Path("/tmp/testMapFileIndexingMap"), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }
}
