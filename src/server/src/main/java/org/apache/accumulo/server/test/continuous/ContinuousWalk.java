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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.accumulo.cloudtrace.instrument.Span;
import org.apache.accumulo.cloudtrace.instrument.Trace;
import org.apache.accumulo.cloudtrace.instrument.Tracer;
import org.apache.accumulo.cloudtrace.instrument.receivers.ZooSpanClient;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.Accumulo;
import org.apache.hadoop.io.Text;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;


public class ContinuousWalk {
  
  private static String debugLog = null;
  
  static class BadChecksumException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;
    
    public BadChecksumException(String msg) {
      super(msg);
    }
    
  }
  
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
    
    if (args.length != 8) {
      throw new IllegalArgumentException("usage : " + ContinuousWalk.class.getName()
          + " [--debug <debug log>] <instance name> <zookeepers> <user> <pass> <table> <min> <max> <sleep time>");
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
    
    Instance instance = new ZooKeeperInstance(instanceName, zooKeepers);
    
    String localhost = InetAddress.getLocalHost().getHostName();
    String path = ZooUtil.getRoot(instance) + Constants.ZTRACERS;
    Tracer.getInstance().addReceiver(new ZooSpanClient(zooKeepers, path, localhost, "cwalk", 1000));
    Accumulo.enableTracing(localhost, "ContinuousWalk");
    Connector conn = instance.getConnector(user, password.getBytes());
    Scanner scanner = conn.createScanner(table, new Authorizations());
    
    Random r = new Random();
    
    ArrayList<Value> values = new ArrayList<Value>();
    
    while (true) {
      String row = findAStartRow(min, max, scanner, r);
      
      while (row != null) {
        
        values.clear();
        
        long t1 = System.currentTimeMillis();
        Span span = Trace.on("walk");
        try {
          scanner.setRange(new Range(new Text(row)));
          for (Entry<Key,Value> entry : scanner) {
            validate(entry.getKey(), entry.getValue());
            values.add(entry.getValue());
          }
        } finally {
          span.stop();
        }
        long t2 = System.currentTimeMillis();
        
        System.out.printf("SRQ %d %s %d %d\n", t1, row, (t2 - t1), values.size());
        
        if (values.size() > 0) {
          row = getPrevRow(values.get(r.nextInt(values.size())));
        } else {
          System.out.printf("MIS %d %s\n", t1, row);
          System.err.printf("MIS %d %s\n", t1, row);
          row = null;
        }
        
        if (sleepTime > 0)
          Thread.sleep(sleepTime);
      }
      
      if (sleepTime > 0)
        Thread.sleep(sleepTime);
    }
  }
  
  private static String findAStartRow(long min, long max, Scanner scanner, Random r) {
    
    byte[] scanStart = ContinuousIngest.genRow(min, max, r);
    scanner.setRange(new Range(new Text(scanStart), null));
    scanner.setBatchSize(100);
    
    int count = 0;
    String pr = null;
    
    long t1 = System.currentTimeMillis();
    
    for (Entry<Key,Value> entry : scanner) {
      validate(entry.getKey(), entry.getValue());
      pr = getPrevRow(entry.getValue());
      count++;
      if (pr != null)
        break;
    }
    
    long t2 = System.currentTimeMillis();
    
    System.out.printf("FSR %d %s %d %d\n", t1, new String(scanStart), (t2 - t1), count);
    
    return pr;
  }
  
  static int getPrevRowOffset(byte val[]) {
    if (val.length == 0)
      throw new IllegalArgumentException();
    if (val[53] != ':')
      throw new IllegalArgumentException(new String(val));
    
    // prev row starts at 54
    if (val[54] != ':') {
      if (val[54 + 16] != ':')
        throw new IllegalArgumentException(new String(val));
      return 54;
    }
    
    return -1;
  }
  
  static String getPrevRow(Value value) {
    
    byte[] val = value.get();
    int offset = getPrevRowOffset(val);
    if (offset > 0) {
      return new String(val, offset, 16);
    }
    
    return null;
  }
  
  static int getChecksumOffset(byte val[]) {
    if (val[val.length - 1] != ':') {
      if (val[val.length - 9] != ':')
        throw new IllegalArgumentException(new String(val));
      return val.length - 8;
    }
    
    return -1;
  }
  
  static void validate(Key key, Value value) throws BadChecksumException {
    int ckOff = getChecksumOffset(value.get());
    if (ckOff < 0)
      return;
    
    long storedCksum = Long.parseLong(new String(value.get(), ckOff, 8), 16);
    
    CRC32 cksum = new CRC32();
    
    cksum.update(key.getRowData().toArray());
    cksum.update(key.getColumnFamilyData().toArray());
    cksum.update(key.getColumnQualifierData().toArray());
    cksum.update(value.get(), 0, ckOff);
    
    if (cksum.getValue() != storedCksum) {
      throw new BadChecksumException("Checksum invalid " + key + " " + value);
    }
  }
}
