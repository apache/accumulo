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

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.cli.ClientOnRequiredTable;
import org.apache.accumulo.server.client.HdfsZooInstance;
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
    List<String> args = new ArrayList<String>();
  }

  public static void main(String[] args) throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    final FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    Opts opts = new Opts();
    if (args.length == 5) {
      System.err.println("Deprecated syntax for BulkImportDirectory, please use the new style (see --help)");
      final String user = args[0];
      final byte[] pass = args[1].getBytes(UTF_8);
      final String tableName = args[2];
      final String dir = args[3];
      final String failureDir = args[4];
      final Path failureDirPath = new Path(failureDir);
      fs.delete(failureDirPath, true);
      fs.mkdirs(failureDirPath);
      HdfsZooInstance.getInstance().getConnector(user, new PasswordToken(pass)).tableOperations().importDirectory(tableName, dir, failureDir, false);
    } else {
      opts.parseArgs(BulkImportDirectory.class.getName(), args);
      fs.delete(new Path(opts.failures), true);
      fs.mkdirs(new Path(opts.failures));
      opts.getConnector().tableOperations().importDirectory(opts.tableName, opts.source, opts.failures, false);
    }
  }
}
