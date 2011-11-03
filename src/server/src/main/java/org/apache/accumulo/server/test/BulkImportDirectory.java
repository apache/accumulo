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
import org.apache.accumulo.core.client.impl.HdfsZooInstance;

public class BulkImportDirectory {
  public static void main(String[] args) throws IOException, AccumuloException, AccumuloSecurityException {
    if (args.length != 5)
      throw new RuntimeException("Usage: bin/accumulo " + BulkImportDirectory.class.getName() + " <username> <password> <tablename> <sourcedir> <failuredir>");
    
    String user = args[0];
    byte[] pass = args[1].getBytes();
    String tableName = args[2];
    String dir = args[3];
    String failureDir = args[4];
    
    int numMapThreads = 4;
    int numAssignThreads = 20;
    boolean disableGC = false;
    
    HdfsZooInstance.getInstance().getConnector(user, pass).tableOperations()
        .importDirectory(tableName, dir, failureDir, numMapThreads, numAssignThreads, disableGC);
  }
}
