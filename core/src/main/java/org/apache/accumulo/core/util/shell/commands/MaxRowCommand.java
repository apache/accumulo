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

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;

public class MaxRowCommand extends ScanCommand {
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      IOException, ParseException {
    String tableName = OptUtil.getTableOpt(cl, shellState);
    Range range = getRange(cl);
    Authorizations auths = getAuths(cl, shellState);
    Text startRow = range.getStartKey() == null ? null : range.getStartKey().getRow();
    Text endRow = range.getEndKey() == null ? null : range.getEndKey().getRow();
    
    try {
      Text max = shellState.getConnector().tableOperations()
          .getMaxRow(tableName, auths, startRow, range.isStartKeyInclusive(), endRow, range.isEndKeyInclusive());
      if (max != null)
        shellState.getReader().printString(max.toString() + "\n");
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    return 0;
  }
  
  @Override
  public String description() {
    return "finds the max row in a table within a given range";
  }
}
