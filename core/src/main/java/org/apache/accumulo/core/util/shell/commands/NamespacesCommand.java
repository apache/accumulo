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
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.iterators.AbstractIteratorDecorator;

public class NamespacesCommand extends Command {
  private Option disablePaginationOpt, namespaceIdOption;
  
  @SuppressWarnings("unchecked")
  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
      Iterator<String> names = shellState.getConnector().tableNamespaceOperations().list().iterator();
      Iterator<String> ids = new NamespaceIdIterator(new TreeMap<String,String>(shellState.getConnector().tableNamespaceOperations().namespaceIdMap()).entrySet().iterator());
    
    if (cl.hasOption(namespaceIdOption.getOpt())) {
      shellState.printLines(ids, !cl.hasOption(disablePaginationOpt.getOpt()));
    } else {
      shellState.printLines(names, !cl.hasOption(disablePaginationOpt.getOpt()));
    }
    return 0;
  }
  
  /**
   * Decorator that formats the id and name for display.
   */
  private static final class NamespaceIdIterator extends AbstractIteratorDecorator {
    public NamespaceIdIterator(Iterator<Entry<String,String>> iterator) {
      super(iterator);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public Object next() {
      Entry entry = (Entry) super.next();
      return String.format("%-15s => %10s%n", entry.getKey(), entry.getValue());
    }
  }
  
  @Override
  public String description() {
    return "displays a list of all existing table namespaces";
  }
  
  @Override
  public Options getOptions() {
    final Options o = new Options();
    namespaceIdOption = new Option("l", "list-ids", false, "display internal table namespace ids along with the name");
    o.addOption(namespaceIdOption);
    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    o.addOption(disablePaginationOpt);
    return o;
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
}
