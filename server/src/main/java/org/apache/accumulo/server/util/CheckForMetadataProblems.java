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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class CheckForMetadataProblems {
  private static String user;
  private static byte[] pass;
  private static boolean fix = false;
  private static boolean offline = false;
  
  private static boolean sawProblems = false;
  
  public static void checkTable(String tablename, TreeSet<KeyExtent> tablets, boolean patch) {
    // sanity check of metadata table entries
    // make sure tablets has no holes, and that it starts and ends w/ null
    
    if (tablets.size() == 0) {
      System.out.println("No entries found in metadata table for table " + tablename);
      sawProblems = true;
      return;
    }
    
    if (tablets.first().getPrevEndRow() != null) {
      System.out.println("First entry for table " + tablename + "- " + tablets.first() + " - has non null prev end row");
      sawProblems = true;
      return;
    }
    
    if (tablets.last().getEndRow() != null) {
      System.out.println("Last entry for table " + tablename + "- " + tablets.last() + " - has non null end row");
      sawProblems = true;
      return;
    }
    
    Iterator<KeyExtent> tabIter = tablets.iterator();
    Text lastEndRow = tabIter.next().getEndRow();
    boolean everythingLooksGood = true;
    while (tabIter.hasNext()) {
      KeyExtent tabke = tabIter.next();
      boolean broke = false;
      if (tabke.getPrevEndRow() == null) {
        System.out.println("Table " + tablename + " has null prev end row in middle of table " + tabke);
        broke = true;
      } else if (!tabke.getPrevEndRow().equals(lastEndRow)) {
        System.out.println("Table " + tablename + " has a hole " + tabke.getPrevEndRow() + " != " + lastEndRow);
        broke = true;
      }
      if (broke) {
        everythingLooksGood = false;
      }
      if (broke && patch) {
        KeyExtent ke = new KeyExtent(tabke);
        ke.setPrevEndRow(lastEndRow);
        MetadataTable.updateTabletPrevEndRow(ke, new AuthInfo(user, ByteBuffer.wrap(pass), HdfsZooInstance.getInstance().getInstanceID()));
        System.out.println("KE " + tabke + " has been repaired to " + ke);
      }
      
      lastEndRow = tabke.getEndRow();
    }
    if (everythingLooksGood)
      System.out.println("All is well for table " + tablename);
    else
      sawProblems = true;
  }
  
  public static void checkMetadataTableEntries(ServerConfiguration conf, FileSystem fs, boolean offline, boolean patch) throws Exception {
    Map<String,TreeSet<KeyExtent>> tables = new HashMap<String,TreeSet<KeyExtent>>();
    
    Scanner scanner;
    
    if (offline) {
      scanner = new OfflineMetadataScanner(conf.getConfiguration(), fs);
    } else {
      scanner = new ScannerImpl(conf.getInstance(), new AuthInfo(user, ByteBuffer.wrap(pass), conf.getInstance().getInstanceID()),
          Constants.METADATA_TABLE_ID, Constants.NO_AUTHS);
    }
    
    scanner.setRange(Constants.METADATA_KEYSPACE);
    ColumnFQ.fetch(scanner, Constants.METADATA_PREV_ROW_COLUMN);
    scanner.fetchColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
    
    Text colf = new Text();
    Text colq = new Text();
    boolean justLoc = false;
    
    int count = 0;
    
    for (Entry<Key,Value> entry : scanner) {
      colf = entry.getKey().getColumnFamily(colf);
      colq = entry.getKey().getColumnQualifier(colq);
      
      count++;
      
      String tableName = (new KeyExtent(entry.getKey().getRow(), (Text) null)).getTableId().toString();
      
      TreeSet<KeyExtent> tablets = tables.get(tableName);
      if (tablets == null) {
        Set<Entry<String,TreeSet<KeyExtent>>> es = tables.entrySet();
        
        for (Entry<String,TreeSet<KeyExtent>> entry2 : es) {
          checkTable(entry2.getKey(), entry2.getValue(), patch);
        }
        
        tables.clear();
        
        tablets = new TreeSet<KeyExtent>();
        tables.put(tableName, tablets);
      }
      
      if (Constants.METADATA_PREV_ROW_COLUMN.equals(colf, colq)) {
        KeyExtent tabletKe = new KeyExtent(entry.getKey().getRow(), entry.getValue());
        tablets.add(tabletKe);
        justLoc = false;
      } else if (colf.equals(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY)) {
        if (justLoc) {
          System.out.println("Problem at key " + entry.getKey());
          sawProblems = true;
          if (patch) {
            Writer t = MetadataTable.getMetadataTable(new AuthInfo(user, ByteBuffer.wrap(pass), HdfsZooInstance.getInstance().getInstanceID()));
            Key k = entry.getKey();
            Mutation m = new Mutation(k.getRow());
            m.putDelete(k.getColumnFamily(), k.getColumnQualifier());
            try {
              t.update(m);
              System.out.println("Deleted " + k);
            } catch (ConstraintViolationException e) {
              e.printStackTrace();
            }
          }
        }
        justLoc = true;
      }
    }
    
    if (count == 0) {
      System.err.println("ERROR : " + Constants.METADATA_TABLE_NAME + " table is empty");
      sawProblems = true;
    }
    
    Set<Entry<String,TreeSet<KeyExtent>>> es = tables.entrySet();
    
    for (Entry<String,TreeSet<KeyExtent>> entry : es) {
      checkTable(entry.getKey(), entry.getValue(), patch);
    }
    
    // end METADATA table sanity check
  }
  
  private static String[] processOptions(String[] args) {
    ArrayList<String> al = new ArrayList<String>();
    
    for (String s : args) {
      if (s.equals("--debug")) {
        enableDebug();
      } else if (s.equals("--fix")) {
        fix = true;
      } else if (s.equals("--offline")) {
        offline = true;
      } else {
        al.add(s);
      }
    }
    
    if (offline && fix) {
      throw new IllegalArgumentException("Cannot fix in offline mode");
    }
    
    return al.toArray(new String[al.size()]);
  }
  
  private static void enableDebug() {
    Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME);
    logger.setLevel(Level.TRACE);
  }
  
  public static void main(String[] args) throws Exception {
    args = processOptions(args);
    
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    Instance instance = HdfsZooInstance.getInstance();
    ServerConfiguration conf = new ServerConfiguration(instance);

    if (args.length == 2) {
      user = args[0];
      pass = args[1].getBytes();
      checkMetadataTableEntries(conf, fs, offline, fix);
    } else if (args.length == 0 && offline) {
      checkMetadataTableEntries(conf, fs, offline, fix);
    } else {
      System.out.println("Usage: " + CheckForMetadataProblems.class.getName() + " (--offline)|([--debug] [--fix] <username> <password>)");
      System.exit(-1);
    }
    
    if (sawProblems)
      System.exit(-1);
  }
  
}
