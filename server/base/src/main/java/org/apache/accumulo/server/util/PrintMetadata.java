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

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class PrintMetadata implements KeywordExecutable {
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
    var serverContext = opts.getServerContext();
    var ample = serverContext.getAmple();
    var tableNameToIdMap = serverContext.tableOperations().tableIdMap();

    opts.tables.forEach(tableName -> {
      String tableId = tableNameToIdMap.get(tableName);
      var tm = ample.readTablets().forTable(TableId.of(tableId)).build();
      for (var tablet : tm) {
        System.out.println(tablet);
      }
    });
  }
}
