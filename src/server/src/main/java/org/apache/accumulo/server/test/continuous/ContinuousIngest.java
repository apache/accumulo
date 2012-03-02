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
import java.util.Random;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.accumulo.cloudtrace.instrument.CountSampler;
import org.apache.accumulo.cloudtrace.instrument.Trace;
import org.apache.accumulo.cloudtrace.instrument.Tracer;
import org.apache.accumulo.cloudtrace.instrument.receivers.ZooSpanClient;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.test.FastFormat;
import org.apache.hadoop.io.Text;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;


public class ContinuousIngest {
  
  private static String debugLog = null;
  private static final byte[] EMPTY_BYTES = new byte[0];
  
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
    
    if (args.length != 13) {
      throw new IllegalArgumentException(
          "usage : "
              + ContinuousIngest.class.getName()
              + " [--debug <debug log>] <instance name> <zookeepers> <user> <pass> <table> <min> <max> <max colf> <max colq> <max mem> <max latency> <max threads> <enable checksum>");
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
    short maxColF = Short.parseShort(args[7]);
    short maxColQ = Short.parseShort(args[8]);
    
    long maxMemory = Long.parseLong(args[9]);
    long maxLatency = Integer.parseInt(args[10]);
    int maxWriteThreads = Integer.parseInt(args[11]);
    
    boolean checksum = Boolean.parseBoolean(args[12]);
    
    if (min < 0 || max < 0 || max <= min) {
      throw new IllegalArgumentException("bad min and max");
    }
    Instance instance = new ZooKeeperInstance(instanceName, zooKeepers);
    Connector conn = instance.getConnector(user, password);
    String localhost = InetAddress.getLocalHost().getHostName();
    String path = ZooUtil.getRoot(instance) + Constants.ZTRACERS;
    Tracer.getInstance().addReceiver(new ZooSpanClient(zooKeepers, path, localhost, "cingest", 1000));
    
    BatchWriter bw = conn.createBatchWriter(table, maxMemory, maxLatency, maxWriteThreads);
    bw = Trace.wrapAll(bw, new CountSampler(1024));
    
    Random r = new Random();
    
    byte[] ingestInstanceId = UUID.randomUUID().toString().getBytes();
    
    System.out.printf("UUID %d %s\n", System.currentTimeMillis(), new String(ingestInstanceId));
    
    long count = 0;
    final int flushInterval = 1000000;
    final int maxDepth = 25;
    
    // always want to point back to flushed data. This way the previous item should
    // always exist in accumulo when verifying data. To do this make insert N point
    // back to the row from insert (N - flushInterval). The array below is used to keep
    // track of this.
    long prevRows[] = new long[flushInterval];
    long firstRows[] = new long[flushInterval];
    int firstColFams[] = new int[flushInterval];
    int firstColQuals[] = new int[flushInterval];
    
    long lastFlushTime = System.currentTimeMillis();
    
    while (true) {
      // generate first set of nodes
      for (int index = 0; index < flushInterval; index++) {
        long rowLong = genLong(min, max, r);
        prevRows[index] = rowLong;
        firstRows[index] = rowLong;
        
        int cf = r.nextInt(maxColF);
        int cq = r.nextInt(maxColQ);
        
        firstColFams[index] = cf;
        firstColQuals[index] = cq;
        
        Mutation m = genMutation(rowLong, cf, cq, ingestInstanceId, count, null, r, checksum);
        count++;
        bw.addMutation(m);
      }
      
      lastFlushTime = flush(bw, count, flushInterval, lastFlushTime);
      
      // generate subsequent sets of nodes that link to previous set of nodes
      for (int depth = 1; depth < maxDepth; depth++) {
        for (int index = 0; index < flushInterval; index++) {
          long rowLong = genLong(min, max, r);
          byte[] prevRow = genRow(prevRows[index]);
          prevRows[index] = rowLong;
          Mutation m = genMutation(rowLong, r.nextInt(maxColF), r.nextInt(maxColQ), ingestInstanceId, count, prevRow, r, checksum);
          count++;
          bw.addMutation(m);
        }
        
        lastFlushTime = flush(bw, count, flushInterval, lastFlushTime);
      }
      
      // create one big linked list, this makes all of the first inserts
      // point to something
      for (int index = 0; index < flushInterval - 1; index++) {
        Mutation m = genMutation(firstRows[index], firstColFams[index], firstColQuals[index], ingestInstanceId, count, genRow(prevRows[index + 1]), r, checksum);
        count++;
        bw.addMutation(m);
      }
      lastFlushTime = flush(bw, count, flushInterval, lastFlushTime);
    }
  }
  
  private static long flush(BatchWriter bw, long count, final int flushInterval, long lastFlushTime) throws MutationsRejectedException {
    long t1 = System.currentTimeMillis();
    bw.flush();
    long t2 = System.currentTimeMillis();
    System.out.printf("FLUSH %d %d %d %d %d\n", t2, (t2 - lastFlushTime), (t2 - t1), count, flushInterval);
    lastFlushTime = t2;
    return lastFlushTime;
  }
  
  public static Mutation genMutation(long rowLong, int cfInt, int cqInt, byte[] ingestInstanceId, long count, byte[] prevRow, Random r, boolean checksum) {
    // Adler32 is supposed to be faster, but according to wikipedia is not good for small data.... so used CRC32 instead
    CRC32 cksum = null;
    
    byte[] rowString = genRow(rowLong);
    
    byte[] cfString = FastFormat.toZeroPaddedString(cfInt, 4, 16, EMPTY_BYTES);
    byte[] cqString = FastFormat.toZeroPaddedString(cqInt, 4, 16, EMPTY_BYTES);
    
    if (checksum) {
      cksum = new CRC32();
      cksum.update(rowString);
      cksum.update(cfString);
      cksum.update(cqString);
    }
    
    Mutation m = new Mutation(new Text(rowString));
    
    m.put(new Text(cfString), new Text(cqString), createValue(ingestInstanceId, count, prevRow, cksum));
    return m;
  }
  
  public static final long genLong(long min, long max, Random r) {
    return (Math.abs(r.nextLong()) % (max - min)) + min;
  }
  
  static final byte[] genRow(long min, long max, Random r) {
    return genRow(genLong(min, max, r));
  }
  
  static final byte[] genRow(long rowLong) {
    return FastFormat.toZeroPaddedString(rowLong, 16, 16, EMPTY_BYTES);
  }
  
  private static Value createValue(byte[] ingestInstanceId, long count, byte[] prevRow, Checksum cksum) {
    int dataLen = ingestInstanceId.length + 16 + (prevRow == null ? 0 : prevRow.length) + 3;
    if (cksum != null)
      dataLen += 8;
    byte val[] = new byte[dataLen];
    System.arraycopy(ingestInstanceId, 0, val, 0, ingestInstanceId.length);
    int index = ingestInstanceId.length;
    val[index++] = ':';
    int added = FastFormat.toZeroPaddedString(val, index, count, 16, 16, EMPTY_BYTES);
    if (added != 16)
      throw new RuntimeException(" " + added);
    index += 16;
    val[index++] = ':';
    if (prevRow != null) {
      System.arraycopy(prevRow, 0, val, index, prevRow.length);
      index += prevRow.length;
    }
    
    val[index++] = ':';
    
    if (cksum != null) {
      cksum.update(val, 0, index);
      cksum.getValue();
      FastFormat.toZeroPaddedString(val, index, cksum.getValue(), 8, 16, EMPTY_BYTES);
    }
    
    // System.out.println("val "+new String(val));
    
    return new Value(val);
  }
}
