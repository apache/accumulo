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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class TestRandomDeletes {
  private static final Logger log = Logger.getLogger(TestRandomDeletes.class);
  private static Authorizations auths = new Authorizations("L1", "L2", "G1", "GROUP2");
  
  private static AuthInfo credentials;
  
  static private class RowColumn implements Comparable<RowColumn> {
    Text row;
    Column column;
    long timestamp;
    
    public RowColumn(Text row, Column column, long timestamp) {
      this.row = row;
      this.column = column;
      this.timestamp = timestamp;
    }
    
    public int compareTo(RowColumn other) {
      int result = row.compareTo(other.row);
      if (result != 0)
        return result;
      return column.compareTo(other.column);
    }
    
    public String toString() {
      return row.toString() + ":" + column.toString();
    }
  }
  
  private static TreeSet<RowColumn> scanAll(Text t) throws Exception {
    TreeSet<RowColumn> result = new TreeSet<RowColumn>();
    Connector conn = HdfsZooInstance.getInstance().getConnector(credentials.user, credentials.password);
    Scanner scanner = conn.createScanner(t.toString(), auths);
    scanner.setBatchSize(20000);
    for (Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      Column column = new Column(TextUtil.getBytes(key.getColumnFamily()), TextUtil.getBytes(key.getColumnQualifier()), TextUtil.getBytes(key
          .getColumnVisibility()));
      result.add(new RowColumn(key.getRow(), column, key.getTimestamp()));
    }
    return result;
  }
  
  private static long scrambleDeleteHalfAndCheck(Text t, Set<RowColumn> rows) throws Exception {
    int result = 0;
    ArrayList<RowColumn> entries = new ArrayList<RowColumn>(rows);
    java.util.Collections.shuffle(entries);
    
    Connector connector = HdfsZooInstance.getInstance().getConnector(credentials.user, credentials.password);
    BatchWriter mutations = connector.createBatchWriter(t.toString(), 10000l, 10000l, 4);
    ColumnVisibility cv = new ColumnVisibility("L1&L2&G1&GROUP2");
    
    for (int i = 0; i < (entries.size() + 1) / 2; i++) {
      RowColumn rc = entries.get(i);
      Mutation m = new Mutation(rc.row);
      m.putDelete(new Text(rc.column.columnFamily), new Text(rc.column.columnQualifier), cv, rc.timestamp + 1);
      mutations.addMutation(m);
      rows.remove(rc);
      result++;
    }
    
    mutations.close();
    
    Set<RowColumn> current = scanAll(t);
    current.removeAll(rows);
    if (current.size() > 0) {
      throw new RuntimeException(current.size() + " records not deleted");
    }
    return result;
  }
  
  static public void main(String[] args) {
    Option usernameOpt = new Option("username", "username", true, "username");
    Option passwordOpt = new Option("password", "password", true, "password");
    
    Options opts = new Options();
    
    opts.addOption(usernameOpt);
    opts.addOption(passwordOpt);
    
    Parser p = new BasicParser();
    CommandLine cl = null;
    try {
      cl = p.parse(opts, args);
    } catch (ParseException e1) {
      System.out.println("Parse Exception, exiting.");
      return;
    }
    credentials = new AuthInfo(cl.getOptionValue("username", "root"), ByteBuffer.wrap(cl.getOptionValue("password", "secret").getBytes()), HdfsZooInstance
        .getInstance().getInstanceID());
    
    try {
      long deleted = 0;
      
      Text t = new Text("test_ingest");
      
      TreeSet<RowColumn> doomed = scanAll(t);
      log.info("Got " + doomed.size() + " rows");
      
      long startTime = System.currentTimeMillis();
      while (true) {
        long half = scrambleDeleteHalfAndCheck(t, doomed);
        deleted += half;
        if (half == 0)
          break;
      }
      long stopTime = System.currentTimeMillis();
      
      long elapsed = (stopTime - startTime) / 1000;
      log.info("deleted " + deleted + " values in " + elapsed + " seconds");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
