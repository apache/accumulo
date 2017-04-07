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
package org.apache.accumulo.shell.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.ShellCommandException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class SetScanIterCommand extends SetIterCommand {
  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, IOException, ShellCommandException {
    Shell.log.warn("Deprecated, use " + new SetShellIterCommand().getName());
    return super.execute(fullCommand, cl, shellState);
  }

  @Override
  protected void setTableProperties(final CommandLine cl, final Shell shellState, final int priority, final Map<String,String> options, final String classname,
      final String name) throws AccumuloException, AccumuloSecurityException, ShellCommandException, TableNotFoundException {

    final String tableName = OptUtil.getTableOpt(cl, shellState);

    ScanCommand.ensureTserversCanLoadIterator(shellState, tableName, classname);

    for (Iterator<Entry<String,String>> i = options.entrySet().iterator(); i.hasNext();) {
      final Entry<String,String> entry = i.next();
      if (entry.getValue() == null || entry.getValue().isEmpty()) {
        i.remove();
      }
    }

    List<IteratorSetting> tableScanIterators = shellState.scanIteratorOptions.get(tableName);
    if (tableScanIterators == null) {
      tableScanIterators = new ArrayList<>();
      shellState.scanIteratorOptions.put(tableName, tableScanIterators);
    }
    final IteratorSetting setting = new IteratorSetting(priority, name, classname);
    setting.addOptions(options);

    // initialize a scanner to ensure the new setting does not conflict with existing settings
    final String user = shellState.getConnector().whoami();
    final Authorizations auths = shellState.getConnector().securityOperations().getUserAuthorizations(user);
    final Scanner scanner = shellState.getConnector().createScanner(tableName, auths);
    for (IteratorSetting s : tableScanIterators) {
      scanner.addScanIterator(s);
    }
    scanner.addScanIterator(setting);

    // if no exception has been thrown, it's safe to add it to the list
    tableScanIterators.add(setting);
    Shell.log.debug("Scan iterators :" + shellState.scanIteratorOptions.get(tableName));
  }

  @Override
  public String description() {
    return "sets a table-specific scan iterator for this shell session";
  }

  @Override
  public Options getOptions() {
    // Remove the options that specify which type of iterator this is, since
    // they are all scan iterators with this command.
    final HashSet<OptionGroup> groups = new HashSet<>();
    final Options parentOptions = super.getOptions();
    final Options modifiedOptions = new Options();
    for (Iterator<?> it = parentOptions.getOptions().iterator(); it.hasNext();) {
      Option o = (Option) it.next();
      if (!IteratorScope.majc.name().equals(o.getOpt()) && !IteratorScope.minc.name().equals(o.getOpt()) && !IteratorScope.scan.name().equals(o.getOpt())) {
        modifiedOptions.addOption(o);
        OptionGroup group = parentOptions.getOptionGroup(o);
        if (group != null)
          groups.add(group);
      }
    }
    for (OptionGroup group : groups) {
      modifiedOptions.addOptionGroup(group);
    }
    return modifiedOptions;
  }

}
