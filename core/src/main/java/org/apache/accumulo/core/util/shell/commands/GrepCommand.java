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
import java.util.Collections;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class GrepCommand extends ScanCommand {
  
  private Option numThreadsOpt;
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      IOException, MissingArgumentException {
    
    String tableName = OptUtil.getTableOpt(cl, shellState);
    
    if (cl.getArgList().isEmpty())
      throw new MissingArgumentException("No terms specified");
    
    // handle first argument, if present, the authorizations list to
    // scan with
    int numThreads = 20;
    if (cl.hasOption(numThreadsOpt.getOpt())) {
      numThreads = Integer.parseInt(cl.getOptionValue(numThreadsOpt.getOpt()));
    }
    Authorizations auths = getAuths(cl, shellState);
    BatchScanner scanner = shellState.getConnector().createBatchScanner(tableName, auths, numThreads);
    scanner.setRanges(Collections.singletonList(getRange(cl)));
    
    for (int i = 0; i < cl.getArgs().length; i++)
      setUpIterator(Integer.MAX_VALUE - cl.getArgs().length + i, "grep" + i, cl.getArgs()[i], scanner);
    
    try {
      // handle columns
      fetchColumns(cl, scanner);
      
      // output the records
      printRecords(cl, shellState, scanner);
    } finally {
      scanner.close();
    }
    
    return 0;
  }
  
  protected void setUpIterator(int prio, String name, String term, BatchScanner scanner) throws IOException {
    if (prio < 0)
      throw new IllegalArgumentException("Priority < 0 " + prio);
    
    IteratorSetting grep = new IteratorSetting(prio, name, GrepIterator.class);
    GrepIterator.setTerm(grep, term);
    scanner.addScanIterator(grep);
  }
  
  @Override
  public String description() {
    return "searches each row, column family, column qualifier and value in a table for a substring (not a regular expression), in parallel, on the server side";
  }
  
  @Override
  public Options getOptions() {
    Options opts = super.getOptions();
    numThreadsOpt = new Option("nt", "num-threads", true, "number of threads to use");
    opts.addOption(numThreadsOpt);
    return opts;
  }
  
  @Override
  public String usage() {
    return getName() + " <term>{ <term>}";
  }
  
  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }
}
