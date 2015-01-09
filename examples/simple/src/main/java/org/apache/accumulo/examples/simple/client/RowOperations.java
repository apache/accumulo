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
package org.apache.accumulo.examples.simple.client;

import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * A demonstration of reading entire rows and deleting entire rows.
 */
public class RowOperations {

  private static final Logger log = Logger.getLogger(RowOperations.class);

  private static Connector connector;
  private static String table = "example";
  private static BatchWriter bw;

  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException,
      MutationsRejectedException {

    ClientOpts opts = new ClientOpts();
    ScannerOpts scanOpts = new ScannerOpts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(RowOperations.class.getName(), args, scanOpts, bwOpts);

    // First the setup work
    connector = opts.getConnector();

    // lets create an example table
    connector.tableOperations().create(table);

    // lets create 3 rows of information
    Text row1 = new Text("row1");
    Text row2 = new Text("row2");
    Text row3 = new Text("row3");

    // Which means 3 different mutations
    Mutation mut1 = new Mutation(row1);
    Mutation mut2 = new Mutation(row2);
    Mutation mut3 = new Mutation(row3);

    // And we'll put 4 columns in each row
    Text col1 = new Text("1");
    Text col2 = new Text("2");
    Text col3 = new Text("3");
    Text col4 = new Text("4");

    // Now we'll add them to the mutations
    mut1.put(new Text("column"), col1, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
    mut1.put(new Text("column"), col2, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
    mut1.put(new Text("column"), col3, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
    mut1.put(new Text("column"), col4, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));

    mut2.put(new Text("column"), col1, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
    mut2.put(new Text("column"), col2, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
    mut2.put(new Text("column"), col3, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
    mut2.put(new Text("column"), col4, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));

    mut3.put(new Text("column"), col1, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
    mut3.put(new Text("column"), col2, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
    mut3.put(new Text("column"), col3, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
    mut3.put(new Text("column"), col4, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));

    // Now we'll make a Batch Writer
    bw = connector.createBatchWriter(table, bwOpts.getBatchWriterConfig());

    // And add the mutations
    bw.addMutation(mut1);
    bw.addMutation(mut2);
    bw.addMutation(mut3);

    // Force a send
    bw.flush();

    // Now lets look at the rows
    Scanner rowThree = getRow(scanOpts, new Text("row3"));
    Scanner rowTwo = getRow(scanOpts, new Text("row2"));
    Scanner rowOne = getRow(scanOpts, new Text("row1"));

    // And print them
    log.info("This is everything");
    printRow(rowOne);
    printRow(rowTwo);
    printRow(rowThree);
    System.out.flush();

    // Now lets delete rowTwo with the iterator
    rowTwo = getRow(scanOpts, new Text("row2"));
    deleteRow(rowTwo);

    // Now lets look at the rows again
    rowThree = getRow(scanOpts, new Text("row3"));
    rowTwo = getRow(scanOpts, new Text("row2"));
    rowOne = getRow(scanOpts, new Text("row1"));

    // And print them
    log.info("This is row1 and row3");
    printRow(rowOne);
    printRow(rowTwo);
    printRow(rowThree);
    System.out.flush();

    // Should only see the two rows
    // Now lets delete rowOne without passing in the iterator

    deleteRow(scanOpts, row1);

    // Now lets look at the rows one last time
    rowThree = getRow(scanOpts, new Text("row3"));
    rowTwo = getRow(scanOpts, new Text("row2"));
    rowOne = getRow(scanOpts, new Text("row1"));

    // And print them
    log.info("This is just row3");
    printRow(rowOne);
    printRow(rowTwo);
    printRow(rowThree);
    System.out.flush();

    // Should only see rowThree

    // Always close your batchwriter

    bw.close();

    // and lets clean up our mess
    connector.tableOperations().delete(table);

    // fin~

  }

  /**
   * Deletes a row given a text object
   */
  private static void deleteRow(ScannerOpts scanOpts, Text row) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    deleteRow(getRow(scanOpts, row));
  }

  /**
   * Deletes a row, given a Scanner of JUST that row
   */
  private static void deleteRow(Scanner scanner) throws MutationsRejectedException {
    Mutation deleter = null;
    // iterate through the keys
    for (Entry<Key,Value> entry : scanner) {
      // create a mutation for the row
      if (deleter == null)
        deleter = new Mutation(entry.getKey().getRow());
      // the remove function adds the key with the delete flag set to true
      deleter.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
    }
    bw.addMutation(deleter);
    bw.flush();
  }

  /**
   * Just a generic print function given an iterator. Not necessarily just for printing a single row
   */
  private static void printRow(Scanner scanner) {
    // iterates through and prints
    for (Entry<Key,Value> entry : scanner)
      log.info("Key: " + entry.getKey().toString() + " Value: " + entry.getValue().toString());
  }

  /**
   * Gets a scanner over one row
   */
  private static Scanner getRow(ScannerOpts scanOpts, Text row) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    // Create a scanner
    Scanner scanner = connector.createScanner(table, Authorizations.EMPTY);
    scanner.setBatchSize(scanOpts.scanBatchSize);
    // Say start key is the one with key of row
    // and end key is the one that immediately follows the row
    scanner.setRange(new Range(row));
    return scanner;
  }

}
