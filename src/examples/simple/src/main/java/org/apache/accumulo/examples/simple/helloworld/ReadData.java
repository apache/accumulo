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
package org.apache.accumulo.examples.simple.helloworld;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * Reads all data between two rows; all data after a given row; or all data in a table, depending on the number of arguments given.
 */
public class ReadData {
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    if (args.length < 5 || args.length > 7) {
      System.out
          .println("bin/accumulo accumulo.examples.helloworld.ReadData <instance name> <zoo keepers> <username> <password> <tablename> [startkey [endkey]]");
      System.exit(1);
    }
    
    String instanceName = args[0];
    String zooKeepers = args[1];
    String user = args[2];
    byte[] pass = args[3].getBytes();
    String tableName = args[4];
    
    ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
    Connector connector = instance.getConnector(user, pass);
    
    Scanner scan = connector.createScanner(tableName, Constants.NO_AUTHS);
    Key start = null;
    if (args.length > 5)
      start = new Key(new Text(args[5]));
    Key end = null;
    if (args.length > 6)
      end = new Key(new Text(args[6]));
    scan.setRange(new Range(start, end));
    Iterator<Entry<Key,Value>> iter = scan.iterator();
    
    while (iter.hasNext()) {
      Entry<Key,Value> e = iter.next();
      Text colf = e.getKey().getColumnFamily();
      Text colq = e.getKey().getColumnQualifier();
      System.out.print("row: " + e.getKey().getRow() + ", colf: " + colf + ", colq: " + colq);
      System.out.println(", value: " + e.getValue().toString());
    }
  }
}
