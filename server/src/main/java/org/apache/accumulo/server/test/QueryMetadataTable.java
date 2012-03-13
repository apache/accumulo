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
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.io.Text;

public class QueryMetadataTable {
  private static AuthInfo credentials;
  
  static String location;
  
  static class MDTQuery implements Runnable {
    private Text row;
    
    MDTQuery(Text row) {
      this.row = row;
    }
    
    public void run() {
      try {
        KeyExtent extent = new KeyExtent(row, (Text) null);
        
        Connector connector = HdfsZooInstance.getInstance().getConnector(credentials.user, credentials.password);
        Scanner mdScanner = connector.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
        Text row = extent.getMetadataEntry();
        
        mdScanner.setRange(new Range(row));
        
        for (Entry<Key,Value> entry : mdScanner) {
          if (!entry.getKey().getRow().equals(row))
            break;
        }
        
      } catch (TableNotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (AccumuloException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }
  
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
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
    
    if (cl.getArgs().length != 2) {
      HelpFormatter hf = new HelpFormatter();
      hf.printHelp("queryMetadataTable <numQueries> <numThreads> ", opts);
      return;
    }
    String[] rargs = cl.getArgs();
    
    int numQueries = Integer.parseInt(rargs[0]);
    int numThreads = Integer.parseInt(rargs[1]);
    credentials = new AuthInfo(cl.getOptionValue("username", "root"), ByteBuffer.wrap(cl.getOptionValue("password", "secret").getBytes()), HdfsZooInstance
        .getInstance().getInstanceID());
    
    Connector connector = HdfsZooInstance.getInstance().getConnector(credentials.user, credentials.password);
    Scanner scanner = connector.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
    scanner.setBatchSize(20000);
    Text mdrow = new Text(KeyExtent.getMetadataEntry(new Text(Constants.METADATA_TABLE_ID), null));
    
    HashSet<Text> rowSet = new HashSet<Text>();
    
    int count = 0;
    
    for (Entry<Key,Value> entry : scanner) {
      System.out.print(".");
      if (count % 72 == 0) {
        System.out.printf(" %,d\n", count);
      }
      if (entry.getKey().compareRow(mdrow) == 0 && entry.getKey().getColumnFamily().compareTo(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY) == 0) {
        System.out.println(entry.getKey() + " " + entry.getValue());
        location = entry.getValue().toString();
      }
      
      if (!entry.getKey().getRow().toString().startsWith(Constants.METADATA_TABLE_ID))
        rowSet.add(entry.getKey().getRow());
      count++;
      
    }
    
    System.out.printf(" %,d\n", count);
    
    ArrayList<Text> rows = new ArrayList<Text>(rowSet);
    
    Random r = new Random();
    
    ExecutorService tp = Executors.newFixedThreadPool(numThreads);
    
    long t1 = System.currentTimeMillis();
    
    for (int i = 0; i < numQueries; i++) {
      int index = r.nextInt(rows.size());
      MDTQuery mdtq = new MDTQuery(rows.get(index));
      tp.submit(mdtq);
    }
    
    tp.shutdown();
    
    try {
      tp.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    
    long t2 = System.currentTimeMillis();
    double delta = (t2 - t1) / 1000.0;
    System.out.println("time : " + delta + "  queries per sec : " + (numQueries / delta));
  }
}
