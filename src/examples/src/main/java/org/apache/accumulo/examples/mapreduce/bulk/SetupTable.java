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
package org.apache.accumulo.examples.mapreduce.bulk;

import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.hadoop.io.Text;

public class SetupTable {
  
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    if (args.length == 5) {
      // create a basic table
      new ZooKeeperInstance(args[0], args[1]).getConnector(args[2], args[3].getBytes()).tableOperations().create(args[4]);
    } else if (args.length > 5) {
      // create a table with initial partitions
      Connector conn = new ZooKeeperInstance(args[0], args[1]).getConnector(args[2], args[3].getBytes());
      
      TreeSet<Text> intialPartitions = new TreeSet<Text>();
      for (int i = 5; i < args.length; ++i)
        intialPartitions.add(new Text(args[i]));
      
      conn.tableOperations().create(args[4]);
      conn.tableOperations().addSplits(args[4], intialPartitions);
    } else {
      System.err.println("Usage : SetupTable <master> <username> <password> <table name> [<split point>{ <split point}]");
    }
  }
}
