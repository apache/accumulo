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

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.LoggerFactory;

public abstract class ShellPluginConfigurationCommand extends Command {
  private Option removePluginOption, pluginClassOption, listPluginOption;

  private String pluginType;

  private Property tableProp;

  private String classOpt;

  ShellPluginConfigurationCommand(final String typeName, final Property tableProp,
      final String classOpt) {
    this.pluginType = typeName;
    this.tableProp = tableProp;
    this.classOpt = classOpt;
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {
    final String tableName = OptUtil.getTableOpt(cl, shellState);

    if (cl.hasOption(removePluginOption.getOpt())) {
      // Remove the property
      removePlugin(cl, shellState, tableName);

      shellState.getWriter().println("Removed " + pluginType + " on " + tableName);
    } else if (cl.hasOption(listPluginOption.getOpt())) {
      // Get the options for this table
      shellState.getAccumuloClient().tableOperations().getConfiguration(tableName)
          .forEach((key, value) -> {
            if (key.startsWith(tableProp.toString())) {
              shellState.getWriter().println(key + ": " + value);
            }
          });
    } else {
      // Set the plugin with the provided options
      String className = cl.getOptionValue(pluginClassOption.getOpt());

      // Set the plugin property on the table
      setPlugin(cl, shellState, tableName, className);
    }

    return 0;
  }

  protected void setPlugin(final CommandLine cl, final Shell shellState, final String tableName,
      final String className) throws AccumuloException, AccumuloSecurityException {
    shellState.getAccumuloClient().tableOperations().setProperty(tableName, tableProp.toString(),
        className);
  }

  protected void removePlugin(final CommandLine cl, final Shell shellState, final String tableName)
      throws AccumuloException, AccumuloSecurityException {
    shellState.getAccumuloClient().tableOperations().removeProperty(tableName,
        tableProp.toString());
  }

  public static <T> Class<? extends T> getPluginClass(final String tableName,
      final Shell shellState, final Class<T> clazz, final Property pluginProp) {
    Map<String,String> props;
    try {
      props = shellState.getAccumuloClient().tableOperations().getConfiguration(tableName);
    } catch (AccumuloException | TableNotFoundException e) {
      return null;
    }

    for (Entry<String,String> ent : props.entrySet()) {
      if (ent.getKey().equals(pluginProp.toString())) {
        Class<? extends T> pluginClazz;
        String[] args = new String[2];
        try {
          Options o = new Options();
          o.addOption(OptUtil.tableOpt());
          args[0] = "-t";
          args[1] = tableName;
          CommandLine cl = new DefaultParser().parse(o, args);
          pluginClazz =
              shellState.getClassLoader(cl, shellState).loadClass(ent.getValue()).asSubclass(clazz);
        } catch (ClassNotFoundException e) {
          LoggerFactory.getLogger(ShellPluginConfigurationCommand.class).error("Class not found {}",
              e.getMessage());
          return null;
        } catch (ParseException e) {
          LoggerFactory.getLogger(ShellPluginConfigurationCommand.class)
              .error("Error parsing table: {} {}", Arrays.toString(args), e.getMessage());
          return null;
        } catch (TableNotFoundException e) {
          LoggerFactory.getLogger(ShellPluginConfigurationCommand.class)
              .error("Table not found: {} {}", tableName, e.getMessage());
          return null;
        } catch (Exception e) {
          LoggerFactory.getLogger(ShellPluginConfigurationCommand.class).error("Error: {}",
              e.getMessage());
          return null;
        }

        return pluginClazz;
      }
    }

    return null;
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    final OptionGroup actionGroup = new OptionGroup();

    pluginClassOption = new Option(classOpt, pluginType, true,
        "fully qualified name of the " + pluginType + " class to use");
    pluginClassOption.setArgName("className");

    // Action to take: apply (default), remove, list
    removePluginOption = new Option("r", "remove", false, "remove the current " + pluginType + "");
    listPluginOption = new Option("l", "list", false, "display the current " + pluginType + "");

    actionGroup.addOption(pluginClassOption);
    actionGroup.addOption(removePluginOption);
    actionGroup.addOption(listPluginOption);
    actionGroup.setRequired(true);

    o.addOptionGroup(actionGroup);
    o.addOption(OptUtil.tableOpt("table to set the " + pluginType + " on"));

    return o;
  }

  @Override
  public int numArgs() {
    return 0;
  }

}
