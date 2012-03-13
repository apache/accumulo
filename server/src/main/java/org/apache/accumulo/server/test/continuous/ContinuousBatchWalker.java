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
package org.apache.accumulo.server.test.continuous;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class ContinuousBatchWalker {
  private static String debugLog = null;
  
  private static String[] processOptions(String[] args) {
    ArrayList<String> al = new ArrayList<String>();
    
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--debug")) {
        debugLog = args[++i];
      } else {
        al.add(args[i]);
      }
    }
    
    return al.toArray(new String[al.size()]);
  }
  
  public static void main(String[] args) throws Exception {
    
    args = processOptions(args);
    
    if (args.length != 10) {
      throw new IllegalArgumentException("usage : " + ContinuousBatchWalker.class.getName()
          + " [--debug <debug log>] <instance name> <zookeepers> <user> <pass> <table> <min> <max> <sleep time> <batch size> <query threads>");
    }
    
    if (debugLog != null) {
      Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME);
      logger.setLevel(Level.TRACE);
      logger.setAdditivity(false);
      logger.addAppender(new FileAppender(new PatternLayout("%d{dd HH:mm:ss,SSS} [%-8c{2}] %-5p: %m%n"), debugLog, true));
    }
    
    String instanceName = args[0];
    String zooKeepers = args[1];
    
    String user = args[2];
    String password = args[3];
    
    String table = args[4];
    
    long min = Long.parseLong(args[5]);
    long max = Long.parseLong(args[6]);
    
    long sleepTime = Long.parseLong(args[7]);
    
    int batchSize = Integer.parseInt(args[8]);
    int numQueryThreads = Integer.parseInt(args[9]);
    
    Connector conn = new ZooKeeperInstance(instanceName, zooKeepers).getConnector(user, password.getBytes());
    Scanner scanner = conn.createScanner(table, Constants.NO_AUTHS);
    BatchScanner bs = conn.createBatchScanner(table, Constants.NO_AUTHS, numQueryThreads);
    
    Random r = new Random();
    
    while (true) {
      Set<Text> batch = getBatch(scanner, min, max, batchSize, r);
      
      List<Range> ranges = new ArrayList<Range>(batch.size());
      
      for (Text row : batch) {
        ranges.add(new Range(row));
      }
      
      runBatchScan(batchSize, bs, batch, ranges);
      
      UtilWaitThread.sleep(sleepTime);
    }
    
  }
  
  /*
   * private static void runSequentialScan(Scanner scanner, List<Range> ranges) { Set<Text> srowsSeen = new HashSet<Text>(); long st1 =
   * System.currentTimeMillis(); int scount = 0; for (Range range : ranges) { scanner.setRange(range);
   * 
   * for (Entry<Key,Value> entry : scanner) { srowsSeen.add(entry.getKey().getRow()); scount++; } }
   * 
   * 
   * long st2 = System.currentTimeMillis(); System.out.println("SRQ "+(st2 - st1)+" "+srowsSeen.size() +" "+scount); }
   */
  
  private static void runBatchScan(int batchSize, BatchScanner bs, Set<Text> batch, List<Range> ranges) {
    bs.setRanges(ranges);
    
    Set<Text> rowsSeen = new HashSet<Text>();
    
    int count = 0;
    
    long t1 = System.currentTimeMillis();
    
    for (Entry<Key,Value> entry : bs) {
      ContinuousWalk.validate(entry.getKey(), entry.getValue());
      
      rowsSeen.add(entry.getKey().getRow());
      
      addRow(batchSize, entry.getValue());
      
      count++;
    }
    
    long t2 = System.currentTimeMillis();
    
    if (!rowsSeen.equals(batch)) {
      HashSet<Text> copy1 = new HashSet<Text>(rowsSeen);
      HashSet<Text> copy2 = new HashSet<Text>(batch);
      
      copy1.removeAll(batch);
      copy2.removeAll(rowsSeen);
      
      System.out.printf("DIF %d %d %d\n", t1, copy1.size(), copy2.size());
      System.err.printf("DIF %d %d %d\n", t1, copy1.size(), copy2.size());
      System.err.println("Extra seen : " + copy1);
      System.err.println("Not seen   : " + copy2);
    } else {
      System.out.printf("BRQ %d %d %d %d %d\n", t1, (t2 - t1), rowsSeen.size(), count, (int) (rowsSeen.size() / ((t2 - t1) / 1000.0)));
    }
    
  }
  
  private static void addRow(int batchSize, Value v) {
    byte[] val = v.get();
    
    int offset = ContinuousWalk.getPrevRowOffset(val);
    if (offset > 1) {
      Text prevRow = new Text();
      prevRow.set(val, offset, 16);
      if (rowsToQuery.size() < 3 * batchSize) {
        rowsToQuery.add(prevRow);
      }
    }
  }
  
  private static HashSet<Text> rowsToQuery = new HashSet<Text>();
  
  private static Set<Text> getBatch(Scanner scanner, long min, long max, int batchSize, Random r) {
    
    while (rowsToQuery.size() < batchSize) {
      byte[] scanStart = ContinuousIngest.genRow(min, max, r);
      scanner.setRange(new Range(new Text(scanStart), null));
      
      int count = 0;
      
      long t1 = System.currentTimeMillis();
      
      Iterator<Entry<Key,Value>> iter = scanner.iterator();
      while (iter.hasNext() && rowsToQuery.size() < 3 * batchSize) {
        Entry<Key,Value> entry = iter.next();
        ContinuousWalk.validate(entry.getKey(), entry.getValue());
        addRow(batchSize, entry.getValue());
        count++;
      }
      
      long t2 = System.currentTimeMillis();
      
      System.out.println("FSB " + t1 + " " + (t2 - t1) + " " + count);
      
      UtilWaitThread.sleep(100);
    }
    
    HashSet<Text> ret = new HashSet<Text>();
    
    Iterator<Text> iter = rowsToQuery.iterator();
    
    for (int i = 0; i < batchSize; i++) {
      ret.add(iter.next());
      iter.remove();
    }
    
    return ret;
  }
  
}
