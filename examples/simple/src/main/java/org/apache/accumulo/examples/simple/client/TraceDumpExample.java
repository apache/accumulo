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

import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.tracer.TraceDump;
import org.apache.accumulo.tracer.TraceDump.Printer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * Example of using the TraceDump class to print a formatted view of a Trace
 *
 */
public class TraceDumpExample {
  private static final Logger log = LoggerFactory.getLogger(TraceDumpExample.class);

  static class Opts extends ClientOnDefaultTable {
    public Opts() {
      super("trace");
    }

    @Parameter(names = {"--traceid"}, description = "The hex string id of a given trace, for example 16cfbbd7beec4ae3")
    public String traceId = "";
  }

  public void dump(Opts opts) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    if (opts.traceId.isEmpty()) {
      throw new IllegalArgumentException("--traceid option is required");
    }

    final Connector conn = opts.getConnector();
    final String principal = opts.getPrincipal();
    final String table = opts.getTableName();
    if (!conn.securityOperations().hasTablePermission(principal, table, TablePermission.READ)) {
      conn.securityOperations().grantTablePermission(principal, table, TablePermission.READ);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      while (!conn.securityOperations().hasTablePermission(principal, table, TablePermission.READ)) {
        log.info("{} didn't propagate read permission on {}", principal, table);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }
    Scanner scanner = conn.createScanner(table, opts.auths);
    scanner.setRange(new Range(new Text(opts.traceId)));
    TraceDump.printTrace(scanner, new Printer() {
      @Override
      public void print(String line) {
        System.out.println(line);
      }
    });
  }

  public static void main(String[] args) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    TraceDumpExample traceDumpExample = new TraceDumpExample();
    Opts opts = new Opts();
    ScannerOpts scannerOpts = new ScannerOpts();
    opts.parseArgs(TraceDumpExample.class.getName(), args, scannerOpts);

    traceDumpExample.dump(opts);
  }

}
