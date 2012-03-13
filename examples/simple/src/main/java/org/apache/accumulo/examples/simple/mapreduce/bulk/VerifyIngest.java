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
package org.apache.accumulo.examples.simple.mapreduce.bulk;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class VerifyIngest {
  private static final Logger log = Logger.getLogger(VerifyIngest.class);
  
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    if (args.length != 7) {
      System.err.println("VerifyIngest <instance name> <zoo keepers> <username> <password> <table> <startRow> <numRows> ");
      return;
    }
    
    String instanceName = args[0];
    String zooKeepers = args[1];
    String user = args[2];
    byte[] pass = args[3].getBytes();
    String table = args[4];
    
    int startRow = Integer.parseInt(args[5]);
    int numRows = Integer.parseInt(args[6]);
    
    Instance instance = new ZooKeeperInstance(instanceName, zooKeepers);
    Connector connector = instance.getConnector(user, pass);
    Scanner scanner = connector.createScanner(table, Constants.NO_AUTHS);
    
    scanner.setRange(new Range(new Text(String.format("row_%08d", startRow)), null));
    
    Iterator<Entry<Key,Value>> si = scanner.iterator();
    
    boolean ok = true;
    
    for (int i = startRow; i < numRows; i++) {
      
      if (si.hasNext()) {
        Entry<Key,Value> entry = si.next();
        
        if (!entry.getKey().getRow().toString().equals(String.format("row_%08d", i))) {
          log.error("unexpected row key " + entry.getKey().getRow().toString() + " expected " + String.format("row_%08d", i));
          ok = false;
        }
        
        if (!entry.getValue().toString().equals(String.format("value_%08d", i))) {
          log.error("unexpected value " + entry.getValue().toString() + " expected " + String.format("value_%08d", i));
          ok = false;
        }
        
      } else {
        log.error("no more rows, expected " + String.format("row_%08d", i));
        ok = false;
        break;
      }
      
    }
    
    if (ok)
      System.out.println("OK");
    
    System.exit(ok ? 0 : 1);
  }
  
}
