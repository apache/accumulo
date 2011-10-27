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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class TestBinaryRows {
  private static String username = "root";
  private static byte[] passwd = "secret".getBytes();
  private static String mode = null;
  private static String table = null;
  private static long start = 0;
  private static long num = 0;
  private static final long byteOnes;
  
  static {
    // safely build Byte.SIZE number of 1s as a long; not that I think Byte.SIZE will ever be anything but 8, but just for fun
    long b = 1;
    for (int i = 0; i < Byte.SIZE; ++i)
      b |= (1L << i);
    byteOnes = b;
  }
  
  static byte[] encodeLong(long l) {
    byte[] ba = new byte[Long.SIZE / Byte.SIZE];
    
    // parse long into a sequence of bytes
    for (int i = 0; i < ba.length; ++i)
      ba[i] = (byte) (byteOnes & (l >>> (Byte.SIZE * (ba.length - i - 1))));
    
    return ba;
  }
  
  static long decodeLong(byte ba[]) {
    // validate byte array
    if (ba.length > Long.SIZE / Byte.SIZE)
      throw new IllegalArgumentException("Byte array of size " + ba.length + " is too big to hold a long");
    
    // build the long from the bytes
    long l = 0;
    for (int i = 0; i < ba.length; ++i)
      l |= (byteOnes & ba[i]) << (Byte.SIZE * (ba.length - i - 1));
    
    return l;
  }
  
  public static void main(String[] args) {
    mode = args[0];
    if (args.length < 4) {
      System.err.println("ERROR : " + mode + " is not a valid operation or insufficient arguments.");
      throw new RuntimeException("config error");
    }
    table = args[1];
    start = Long.parseLong(args[2]);
    num = Long.parseLong(args[3]);
    
    try {
      Connector connector = HdfsZooInstance.getInstance().getConnector(username, passwd);
      
      Logger.getLogger(Constants.CORE_PACKAGE_NAME).setLevel(Level.DEBUG);
      
      if (mode.equals("ingest") || mode.equals("delete")) {
        BatchWriter bw = connector.createBatchWriter(table, 20000000l, 60l, 8);
        boolean delete = mode.equals("delete");
        
        for (long i = 0; i < num; i++) {
          byte[] row = encodeLong(i + start);
          String value = "" + (i + start);
          
          Mutation m = new Mutation(new Text(row));
          if (delete) {
            m.putDelete(new Text("cf"), new Text("cq"));
          } else {
            m.put(new Text("cf"), new Text("cq"), new Value(value.getBytes()));
          }
          bw.addMutation(m);
        }
        
        bw.close();
      } else if (mode.equals("verifyDeleted")) {
        Scanner s = connector.createScanner(table, Constants.NO_AUTHS);
        Key startKey = new Key(encodeLong(start), "cf".getBytes(), "cq".getBytes(), new byte[0], Long.MAX_VALUE);
        Key stopKey = new Key(encodeLong(start + num - 1), "cf".getBytes(), "cq".getBytes(), new byte[0], 0);
        s.setBatchSize(50000);
        s.setRange(new Range(startKey, stopKey));
        
        for (Entry<Key,Value> entry : s) {
          System.err.println("ERROR : saw entries in range that should be deleted ( first value : " + entry.getValue().toString() + ")");
          System.err.println("exiting...");
          System.exit(1);
        }
        
      } else if (mode.equals("verify")) {
        long t1 = System.currentTimeMillis();
        
        Scanner s = connector.createScanner(table, Constants.NO_AUTHS);
        Key startKey = new Key(encodeLong(start), "cf".getBytes(), "cq".getBytes(), new byte[0], Long.MAX_VALUE);
        Key stopKey = new Key(encodeLong(start + num - 1), "cf".getBytes(), "cq".getBytes(), new byte[0], 0);
        s.setBatchSize(50000);
        s.setRange(new Range(startKey, stopKey));
        
        long i = start;
        
        for (Entry<Key,Value> e : s) {
          Key k = e.getKey();
          Value v = e.getValue();
          
          // System.out.println("v = "+v);
          
          checkKeyValue(i, k, v);
          
          i++;
        }
        
        if (i != start + num) {
          System.err.println("ERROR : did not see expected number of rows, saw " + (i - start) + " expected " + num);
          System.err.println("exiting... ARGHHHHHH");
          System.exit(1);
          
        }
        
        long t2 = System.currentTimeMillis();
        
        System.out.printf("time : %9.2f secs\n", ((t2 - t1) / 1000.0));
        System.out.printf("rate : %9.2f entries/sec\n", num / ((t2 - t1) / 1000.0));
        
      } else if (mode.equals("randomLookups")) {
        int numLookups = 1000;
        
        Random r = new Random();
        
        long t1 = System.currentTimeMillis();
        
        for (int i = 0; i < numLookups; i++) {
          long row = (Math.abs(r.nextLong()) % num) + start;
          
          Scanner s = connector.createScanner(table, Constants.NO_AUTHS);
          Key startKey = new Key(encodeLong(row), "cf".getBytes(), "cq".getBytes(), new byte[0], Long.MAX_VALUE);
          Key stopKey = new Key(encodeLong(row), "cf".getBytes(), "cq".getBytes(), new byte[0], 0);
          s.setRange(new Range(startKey, stopKey));
          
          Iterator<Entry<Key,Value>> si = s.iterator();
          
          if (si.hasNext()) {
            Entry<Key,Value> e = si.next();
            Key k = e.getKey();
            Value v = e.getValue();
            
            checkKeyValue(row, k, v);
            
            if (si.hasNext()) {
              System.err.println("ERROR : lookup on " + row + " returned more than one result ");
              System.err.println("exiting...");
              System.exit(1);
            }
            
          } else {
            System.err.println("ERROR : lookup on " + row + " failed ");
            System.err.println("exiting...");
            System.exit(1);
          }
        }
        
        long t2 = System.currentTimeMillis();
        
        System.out.printf("time    : %9.2f secs\n", ((t2 - t1) / 1000.0));
        System.out.printf("lookups : %9d keys\n", numLookups);
        System.out.printf("rate    : %9.2f lookups/sec\n", numLookups / ((t2 - t1) / 1000.0));
        
      } else if (mode.equals("split")) {
        TreeSet<Text> splits = new TreeSet<Text>();
        int shift = (int) start;
        int count = (int) num;
        
        for (long i = 0; i < count; i++) {
          long splitPoint = i << shift;
          
          splits.add(new Text(encodeLong(splitPoint)));
          System.out.printf("added split point 0x%016x  %,12d\n", splitPoint, splitPoint);
        }
        
        connector.tableOperations().create(table);
        connector.tableOperations().addSplits(table, splits);
        
      } else {
        System.err.println("ERROR : " + mode + " is not a valid operation.");
        System.exit(1);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private static void checkKeyValue(long expected, Key k, Value v) throws Exception {
    if (expected != decodeLong(TextUtil.getBytes(k.getRow()))) {
      System.err.println("ERROR : expected row " + expected + " saw " + decodeLong(TextUtil.getBytes(k.getRow())));
      System.err.println("exiting...");
      throw new Exception();
    }
    
    if (!v.toString().equals("" + expected)) {
      System.err.println("ERROR : expected value " + expected + " saw " + v.toString());
      System.err.println("exiting...");
      throw new Exception();
    }
  }
}
