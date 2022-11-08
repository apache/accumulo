/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.shell.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.ShellCommandException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class SetShellIterCommand extends SetIterCommand {

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException,
      ShellCommandException {
    return super.execute(fullCommand, cl, shellState);
  }

  @Override
  protected void setTableProperties(final CommandLine cl, final Shell shellState,
      final int priority, final Map<String,String> options, final String classname,
      final String name) throws AccumuloException, AccumuloSecurityException, ShellCommandException,
      TableNotFoundException {
    // instead of setting table properties, just put the options in a list to use at scan time

    String profile = cl.getOptionValue(profileOpt.getOpt());

    options.values().removeIf(v -> v == null || v.isEmpty());

    List<IteratorSetting> tableScanIterators =
        shellState.iteratorProfiles.computeIfAbsent(profile, k -> new ArrayList<>());
    final IteratorSetting setting = new IteratorSetting(priority, name, classname);
    setting.addOptions(options);

    tableScanIterators.removeIf(iteratorSetting -> iteratorSetting.getName().equals(name));
    tableScanIterators.add(setting);
  }

  @Override
  public String description() {
    return "adds an iterator to a profile for this shell session";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    setBaseOptions(o);
    setProfileOptions(o);
    return o;
  }

  private void setProfileOptions(Options o) {
    profileOpt = new Option("pn", "profile", true, "iterator profile name");
    profileOpt.setRequired(true);
    profileOpt.setArgName("profile");
    o.addOption(profileOpt);
  }

}
