/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.util;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class PrintMetadata implements KeywordExecutable {
  ServerContext context;

  /**
   * For unit testing
   */
  public PrintMetadata(ServerContext context) {
    this.context = context;
  }
  public PrintMetadata() {
    this.context = null;
  }

  static class Opts extends ServerUtilOpts {
    @Parameter(description = " <table> { <table> ... } ")
    List<String> tables = new ArrayList<>();
  }

  @Override
  public String keyword() {
    return "print-md";
  }

  @Override
  public String description() {
    return "Prints the Accumulo metadata table to the command line.";
  }

  public static void main(String[] args) throws Exception {
    new PrintMetadata().execute(args);
  }

  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(keyword(), args);
    if (this.context == null) {
      this.context = opts.getServerContext();
    }
    var ample = context.getAmple();
    var tableNameToIdMap = context.tableOperations().tableIdMap();

    opts.tables.forEach(tableName -> {
      String tableId = tableNameToIdMap.get(tableName);
      try (var tm = ample.readTablets().forTable(TableId.of(tableId)).build()) {
        System.out.println("Table " + tableName + "(" + tableId + "): ");
        System.out.println("Files:");
        for (var tablet : tm) {
          tablet.getFiles().forEach(stf -> System.out.println(stf.getPathStr()));
        }
      }
    });
  }
}
