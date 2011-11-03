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
package org.apache.accumulo.examples.dirlist;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.LongSummation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class Ingest {
  static final Value nullValue = new Value(new byte[0]);
  
  private static void ingest(File src, ColumnVisibility cv, BatchWriter main, BatchWriter index) throws Exception {
    // build main table entry
    String path = null;
    try {
      path = src.getCanonicalPath();
    } catch (IOException e) {
      path = src.getAbsolutePath();
    }
    Mutation m = new Mutation(QueryUtil.getRow(path));
    Text colf = null;
    if (src.isDirectory())
      colf = QueryUtil.DIR_COLF;
    else
      colf = new Text(LongSummation.longToBytes(Long.MAX_VALUE - src.lastModified()));
    m.put(colf, new Text("length"), cv, new Value(Long.toString(src.length()).getBytes()));
    m.put(colf, new Text("hidden"), cv, new Value(Boolean.toString(src.isHidden()).getBytes()));
    m.put(colf, new Text("exec"), cv, new Value(Boolean.toString(src.canExecute()).getBytes()));
    m.put(colf, new Text("lastmod"), cv, new Value(Long.toString(src.lastModified()).getBytes()));
    main.addMutation(m);
    
    // build index table entries
    Text row = QueryUtil.getForwardIndex(path);
    if (row != null) {
      Text p = new Text(QueryUtil.getRow(path));
      m = new Mutation(row);
      m.put(QueryUtil.INDEX_COLF, p, cv, nullValue);
      index.addMutation(m);
      
      row = QueryUtil.getReverseIndex(path);
      m = new Mutation(row);
      m.put(QueryUtil.INDEX_COLF, p, cv, nullValue);
      index.addMutation(m);
    }
  }
  
  private static void recurse(File src, ColumnVisibility cv, BatchWriter main, BatchWriter index) throws Exception {
    // ingest this File
    ingest(src, cv, main, index);
    // recurse into subdirectories
    if (src.isDirectory()) {
      File[] files = src.listFiles();
      if (files == null)
        return;
      for (File child : files) {
        recurse(child, cv, main, index);
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length < 7) {
      System.out.println("usage: " + Ingest.class.getSimpleName() + " <instance> <zoo> <user> <pass> <table> <index table> <visibility> <dir>{ <dir>}");
      System.exit(1);
    }
    
    String instance = args[0];
    String zooKeepers = args[1];
    String user = args[2];
    String pass = args[3];
    String table = args[4];
    String indexTable = args[5];
    byte[] visibility = args[6].getBytes();
    ColumnVisibility colvis = new ColumnVisibility(args[6]);
    
    Connector conn = new ZooKeeperInstance(instance, zooKeepers).getConnector(user, pass.getBytes());
    if (!conn.tableOperations().exists(table))
      conn.tableOperations().create(table);
    if (!conn.tableOperations().exists(indexTable))
      conn.tableOperations().create(indexTable);
      Authorizations auths = conn.securityOperations().getUserAuthorizations(user);
      if (!auths.contains(visibility)) {
        List<byte[]> copy = new ArrayList<byte[]>(auths.getAuthorizations());
        copy.add(visibility);
        try {
          conn.securityOperations().changeUserAuthorizations(user, new Authorizations(copy));
      } catch (Exception ex) {
        System.out.println("Unable to add visiblity to user " + user + ": " + ex);
        System.exit(1);
      }
    }
    BatchWriter mainBW = conn.createBatchWriter(table, 50000000, 300000l, 4);
    BatchWriter indexBW = conn.createBatchWriter(indexTable, 50000000, 300000l, 4);
    for (int i = 7; i < args.length; i++) {
      recurse(new File(args[i]), colvis, mainBW, indexBW);
      
      // fill in parent directory info
      String file = args[i];
      int slashIndex = -1;
      while ((slashIndex = file.lastIndexOf("/")) > 0) {
        file = file.substring(0, slashIndex);
        ingest(new File(file), colvis, mainBW, indexBW);
      }
    }
    ingest(new File("/"), colvis, mainBW, indexBW);
    
    mainBW.close();
    indexBW.close();
  }
}
