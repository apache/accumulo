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
package org.apache.accumulo.utils.metanalysis;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

/**
 * Finds tablet creation events.
 */
public class FindTablet {
  
  static public class Opts extends ClientOpts {
    @Parameter(names = {"-r", "--row"}, required = true, description = "find tablets that contain this row")
    String row = null;
    
    @Parameter(names = "--tableId", required = true, description = "table id")
    String tableId = null;
  }
  
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(FindTablet.class.getName(), args);
    
    findContainingTablets(opts);
  }
  
  private static void findContainingTablets(Opts opts) throws Exception {
    Range range = new KeyExtent(new Text(opts.tableId), null, null).toMetadataRange();
    
    Scanner scanner = opts.getConnector().createScanner("createEvents", opts.auths);
    scanner.setRange(range);
    
    Text row = new Text(opts.row);
    for (Entry<Key,Value> entry : scanner) {
      KeyExtent ke = new KeyExtent(entry.getKey().getRow(), new Value(TextUtil.getBytes(entry.getKey().getColumnFamily())));
      if (ke.contains(row)) {
        System.out.println(entry.getKey().getColumnQualifier() + " " + ke + " " + entry.getValue());
      }
    }
  }
}
