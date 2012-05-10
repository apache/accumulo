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
package org.apache.accumulo.core.util.shell.commands;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;

public class OfflineCommand extends TableOperation {
  @Override
  public String description() {
    return "starts the process of taking table offline";
  }
  
  protected void doTableOp(Shell shellState, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    if (tableName.equals(Constants.METADATA_TABLE_NAME)) {
      Shell.log.info("  You cannot take the " + Constants.METADATA_TABLE_NAME + " offline.");
    } else {
      Shell.log.info("Attempting to begin taking " + tableName + " offline");
      shellState.getConnector().tableOperations().offline(tableName);
    }
  }
}
