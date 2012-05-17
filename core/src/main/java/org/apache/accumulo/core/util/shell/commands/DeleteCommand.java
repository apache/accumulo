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
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class DeleteCommand extends Command {
  private Option deleteOptAuths, timestampOpt;
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      IOException, ConstraintViolationException {
    shellState.checkTableState();
    
    Mutation m = new Mutation(new Text(cl.getArgs()[0].getBytes(Shell.CHARSET)));
    Text colf = new Text(cl.getArgs()[1].getBytes(Shell.CHARSET));
    Text colq = new Text(cl.getArgs()[2].getBytes(Shell.CHARSET));
    
    if (cl.hasOption(deleteOptAuths.getOpt())) {
      ColumnVisibility le = new ColumnVisibility(cl.getOptionValue(deleteOptAuths.getOpt()));
      if (cl.hasOption(timestampOpt.getOpt()))
        m.putDelete(colf, colq, le, Long.parseLong(cl.getOptionValue(timestampOpt.getOpt())));
      else
        m.putDelete(colf, colq, le);
    } else if (cl.hasOption(timestampOpt.getOpt()))
      m.putDelete(colf, colq, Long.parseLong(cl.getOptionValue(timestampOpt.getOpt())));
    else
      m.putDelete(colf, colq);
    
    BatchWriter bw = shellState.getConnector().createBatchWriter(shellState.getTableName(), m.estimatedMemoryUsed() + 0L, 0L, 1);
    bw.addMutation(m);
    bw.close();
    return 0;
  }
  
  @Override
  public String description() {
    return "deletes a record from a table";
  }
  
  @Override
  public String usage() {
    return getName() + " <row> <colfamily> <colqualifier>";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    
    deleteOptAuths = new Option("l", "visibility-label", true, "formatted visibility");
    deleteOptAuths.setArgName("expression");
    o.addOption(deleteOptAuths);
    
    timestampOpt = new Option("ts", "timestamp", true, "timestamp to use for deletion");
    timestampOpt.setArgName("timestamp");
    o.addOption(timestampOpt);
    
    return o;
  }
  
  @Override
  public int numArgs() {
    return 3;
  }
}
