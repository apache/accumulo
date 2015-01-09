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

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ScannerOpts;
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

/**
 * Reads all data between two rows; all data after a given row; or all data in a table, depending on the number of arguments given.
 */
public class ReadData {

  private static final Logger log = Logger.getLogger(ReadData.class);

  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--startKey")
    String startKey;
    @Parameter(names = "--endKey")
    String endKey;
  }

  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(ReadData.class.getName(), args, scanOpts);

    Connector connector = opts.getConnector();

    Scanner scan = connector.createScanner(opts.tableName, opts.auths);
    scan.setBatchSize(scanOpts.scanBatchSize);
    Key start = null;
    if (opts.startKey != null)
      start = new Key(new Text(opts.startKey));
    Key end = null;
    if (opts.endKey != null)
      end = new Key(new Text(opts.endKey));
    scan.setRange(new Range(start, end));
    Iterator<Entry<Key,Value>> iter = scan.iterator();

    while (iter.hasNext()) {
      Entry<Key,Value> e = iter.next();
      Text colf = e.getKey().getColumnFamily();
      Text colq = e.getKey().getColumnQualifier();
      log.trace("row: " + e.getKey().getRow() + ", colf: " + colf + ", colq: " + colq);
      log.trace(", value: " + e.getValue().toString());
    }
  }
}
