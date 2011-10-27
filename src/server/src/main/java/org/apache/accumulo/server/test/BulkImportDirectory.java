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
package org.apache.accumulo.server.test;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BulkImportDirectory {
  public static void main(String[] args) throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    if (args.length != 5)
      throw new RuntimeException("Usage: bin/accumulo " + BulkImportDirectory.class.getName() + " <username> <password> <tablename> <sourcedir> <failuredir>");
    
    final String user = args[0];
    final byte[] pass = args[1].getBytes();
    final String tableName = args[2];
    final String dir = args[3];
    final String failureDir = args[4];
    final Path failureDirPath = new Path(failureDir);
    final FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    fs.delete(failureDirPath, true);
    fs.mkdirs(failureDirPath);
    HdfsZooInstance.getInstance().getConnector(user, pass).tableOperations().importDirectory(tableName, dir, failureDir, false);
  }
}
