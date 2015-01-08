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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.ShellCommandException;
import org.apache.accumulo.core.util.shell.ShellCommandException.ErrorCode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class SetShellIterCommand extends SetIterCommand {
  private Option profileOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, IOException, ShellCommandException {
    return super.execute(fullCommand, cl, shellState);
  }

  @Override
  protected void setTableProperties(final CommandLine cl, final Shell shellState, final int priority, final Map<String,String> options, final String classname,
      final String name) throws AccumuloException, AccumuloSecurityException, ShellCommandException, TableNotFoundException {
    // instead of setting table properties, just put the options in a list to use at scan time

    String profile = cl.getOptionValue(profileOpt.getOpt());

    // instead of setting table properties, just put the options in a list to use at scan time
    Class<?> loadClass;
    try {
      loadClass = getClass().getClassLoader().loadClass(classname);
    } catch (ClassNotFoundException e) {
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, "Unable to load " + classname);
    }
    try {
      loadClass.asSubclass(SortedKeyValueIterator.class);
    } catch (ClassCastException ex) {
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, "xUnable to load " + classname + " as type " + SortedKeyValueIterator.class.getName());
    }

    for (Iterator<Entry<String,String>> i = options.entrySet().iterator(); i.hasNext();) {
      final Entry<String,String> entry = i.next();
      if (entry.getValue() == null || entry.getValue().isEmpty()) {
        i.remove();
      }
    }

    List<IteratorSetting> tableScanIterators = shellState.iteratorProfiles.get(profile);
    if (tableScanIterators == null) {
      tableScanIterators = new ArrayList<IteratorSetting>();
      shellState.iteratorProfiles.put(profile, tableScanIterators);
    }
    final IteratorSetting setting = new IteratorSetting(priority, name, classname);
    setting.addOptions(options);

    Iterator<IteratorSetting> iter = tableScanIterators.iterator();
    while (iter.hasNext()) {
      if (iter.next().getName().equals(name)) {
        iter.remove();
      }
    }

    tableScanIterators.add(setting);
  }

  @Override
  public String description() {
    return "adds an iterator to a profile for this shell session";
  }

  @Override
  public Options getOptions() {
    // Remove the options that specify which type of iterator this is, since
    // they are all scan iterators with this command.
    final HashSet<OptionGroup> groups = new HashSet<OptionGroup>();
    final Options parentOptions = super.getOptions();
    final Options modifiedOptions = new Options();
    for (Iterator<?> it = parentOptions.getOptions().iterator(); it.hasNext();) {
      Option o = (Option) it.next();
      if (!IteratorScope.majc.name().equals(o.getOpt()) && !IteratorScope.minc.name().equals(o.getOpt()) && !IteratorScope.scan.name().equals(o.getOpt())
          && !"table".equals(o.getLongOpt())) {
        modifiedOptions.addOption(o);
        OptionGroup group = parentOptions.getOptionGroup(o);
        if (group != null)
          groups.add(group);
      }
    }
    for (OptionGroup group : groups) {
      modifiedOptions.addOptionGroup(group);
    }

    profileOpt = new Option("pn", "profile", true, "iterator profile name");
    profileOpt.setRequired(true);
    profileOpt.setArgName("profile");

    modifiedOptions.addOption(profileOpt);

    return modifiedOptions;
  }

}
