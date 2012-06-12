/**
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
package org.apache.accumulo.server.metanalysis;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;

/**
 * Finds tablet creation events.
 */
public class FindTablet {
  public static void main(String[] args) throws Exception {
    
    Options options = new Options();
    options.addOption("r", "row", true, "find tablets that contain this row");
    
    GnuParser parser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      if (cmd.getArgs().length != 5) {
        throw new ParseException("Command takes no arguments");
      }
    } catch (ParseException e) {
      System.err.println("Failed to parse command line " + e.getMessage());
      System.err.println();
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(FindTablet.class.getSimpleName() + " <instance> <zookeepers> <user> <pass> <table ID>", options);
      System.exit(-1);
    }
    
    String instance = cmd.getArgs()[0];
    String zookeepers = cmd.getArgs()[1];
    String user = cmd.getArgs()[2];
    String pass = cmd.getArgs()[3];
    String tableID = cmd.getArgs()[4];
    
    ZooKeeperInstance zki = new ZooKeeperInstance(instance, zookeepers);
    Connector conn = zki.getConnector(user, pass);
    
    if (cmd.hasOption('r')) {
      findContainingTablets(conn, tableID, cmd.getOptionValue('r'));
    } else {
      System.err.println("ERROR :  No search criteria given");
    }
  }

  /**
   * @param conn
   * @param tablePrefix
   * @param tableID
   * @param option
   */
  private static void findContainingTablets(Connector conn, String tableID, String row) throws Exception {
    Range range = new KeyExtent(new Text(tableID), null, null).toMetadataRange();

    Scanner scanner = conn.createScanner("createEvents", new Authorizations());
    
    scanner.setRange(range);
    
    for (Entry<Key,Value> entry : scanner) {
      KeyExtent ke = new KeyExtent(entry.getKey().getRow(), new Value(TextUtil.getBytes(entry.getKey().getColumnFamily())));
      if (ke.contains(new Text(row))) {
        System.out.println(entry.getKey().getColumnQualifier() + " " + ke + " " + entry.getValue());
      }
    }
  }
}
