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

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public class VerifyIngest {
  private static final Logger log = Logger.getLogger(VerifyIngest.class);

  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--start-row")
    int startRow = 0;
    @Parameter(names = "--count", required = true, description = "number of rows to verify")
    int numRows = 0;
  }

  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Opts opts = new Opts();
    opts.parseArgs(VerifyIngest.class.getName(), args);

    Connector connector = opts.getConnector();
    Scanner scanner = connector.createScanner(opts.tableName, opts.auths);

    scanner.setRange(new Range(new Text(String.format("row_%08d", opts.startRow)), null));

    Iterator<Entry<Key,Value>> si = scanner.iterator();

    boolean ok = true;

    for (int i = opts.startRow; i < opts.numRows; i++) {

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
  }

}
