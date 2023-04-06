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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class FormatterCommand extends ShellPluginConfigurationCommand {

  private Option interpeterOption;

  public FormatterCommand() {
    super("formatter", Property.TABLE_FORMATTER_CLASS, "f");
  }

  @Override
  public String description() {
    return "specifies a formatter to use for displaying table entries";
  }

  public static Class<? extends Formatter> getCurrentFormatter(final String tableName,
      final Shell shellState) {
    return ShellPluginConfigurationCommand.getPluginClass(tableName, shellState, Formatter.class,
        Property.TABLE_FORMATTER_CLASS);
  }

  @Override
  public Options getOptions() {
    final Options options = super.getOptions();

    interpeterOption = new Option("i", "interpeter", false, "configure class as interpreter also");

    options.addOption(interpeterOption);

    return options;
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void setPlugin(final CommandLine cl, final Shell shellState, final String tableName,
      final String className) throws AccumuloException, AccumuloSecurityException {
    super.setPlugin(cl, shellState, tableName, className);
    if (cl.hasOption(interpeterOption.getOpt())) {
      shellState.getAccumuloClient().tableOperations().setProperty(tableName,
          Property.TABLE_INTERPRETER_CLASS.toString(), className);
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void removePlugin(final CommandLine cl, final Shell shellState, final String tableName)
      throws AccumuloException, AccumuloSecurityException {
    super.removePlugin(cl, shellState, tableName);
    if (cl.hasOption(interpeterOption.getOpt())) {
      shellState.getAccumuloClient().tableOperations().removeProperty(tableName,
          Property.TABLE_INTERPRETER_CLASS.toString());
    }
  }
}
