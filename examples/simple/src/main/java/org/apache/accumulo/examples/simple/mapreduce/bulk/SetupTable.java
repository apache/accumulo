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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

public class SetupTable {

  static class Opts extends ClientOnRequiredTable {
    @Parameter(description = "<split> { <split> ... } ")
    List<String> splits = new ArrayList<String>();
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(SetupTable.class.getName(), args);
    Connector conn = opts.getConnector();
    conn.tableOperations().create(opts.tableName);
    if (!opts.splits.isEmpty()) {
      // create a table with initial partitions
      TreeSet<Text> intialPartitions = new TreeSet<Text>();
      for (String split : opts.splits) {
        intialPartitions.add(new Text(split));
      }
      conn.tableOperations().addSplits(opts.tableName, intialPartitions);
    }
  }
}
