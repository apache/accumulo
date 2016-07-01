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
package org.apache.accumulo.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.cli.ClientOnRequiredTable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;

public class BulkImportDirectory {
  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = {"-s", "--source"}, description = "directory to import from")
    String source = null;
    @Parameter(names = {"-f", "--failures"}, description = "directory to copy failures into: will be deleted before the bulk import")
    String failures = null;
    @Parameter(description = "<username> <password> <tablename> <sourcedir> <failuredir>")
    List<String> args = new ArrayList<>();
  }

  public static void main(String[] args) throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    final FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    Opts opts = new Opts();
    opts.parseArgs(BulkImportDirectory.class.getName(), args);
    fs.delete(new Path(opts.failures), true);
    fs.mkdirs(new Path(opts.failures));
    opts.getConnector().tableOperations().importDirectory(opts.getTableName(), opts.source, opts.failures, false);
  }
}
