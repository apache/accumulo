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
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
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

public class ContinuousScanner {
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
    
    if (args.length != 9) {
      throw new IllegalArgumentException("usage : " + ContinuousScanner.class.getName()
          + " [--debug <debug log>] <instance name> <zookeepers> <user> <pass> <table> <min> <max> <sleep time> <num to scan>");
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
    long distance = 1000000000000l;
    
    long sleepTime = Long.parseLong(args[7]);
    
    int numToScan = Integer.parseInt(args[8]);
    
    Instance instance = new ZooKeeperInstance(instanceName, zooKeepers);
    Connector conn = instance.getConnector(user, password.getBytes());
    Scanner scanner = conn.createScanner(table, Constants.NO_AUTHS);
    
    Random r = new Random();
    
    double delta = Math.min(.05, .05 / (numToScan / 1000.0));
    // System.out.println("Delta "+delta);
    
    while (true) {
      long startRow = ContinuousIngest.genLong(min, max - distance, r);
      byte[] scanStart = ContinuousIngest.genRow(startRow);
      byte[] scanStop = ContinuousIngest.genRow(startRow + distance);
      
      scanner.setRange(new Range(new Text(scanStart), new Text(scanStop)));
      
      int count = 0;
      Iterator<Entry<Key,Value>> iter = scanner.iterator();
      
      long t1 = System.currentTimeMillis();
      
      while (iter.hasNext()) {
        Entry<Key,Value> entry = iter.next();
        ContinuousWalk.validate(entry.getKey(), entry.getValue());
        count++;
      }
      
      long t2 = System.currentTimeMillis();
      
      // System.out.println("P1 " +count +" "+((1-delta) * numToScan)+" "+((1+delta) * numToScan)+" "+numToScan);
      
      if (count < (1 - delta) * numToScan || count > (1 + delta) * numToScan) {
        if (count == 0) {
          distance = distance * 10;
          if (distance < 0)
            distance = 1000000000000l;
        } else {
          double ratio = (double) numToScan / count;
          // move ratio closer to 1 to make change slower
          ratio = ratio - (ratio - 1.0) * (2.0 / 3.0);
          distance = (long) (ratio * distance);
        }
        
        // System.out.println("P2 "+delta +" "+numToScan+" "+distance+"  "+((double)numToScan/count ));
      }
      
      System.out.printf("SCN %d %s %d %d\n", t1, new String(scanStart), (t2 - t1), count);
      
      if (sleepTime > 0)
        UtilWaitThread.sleep(sleepTime);
    }
    
  }
}
