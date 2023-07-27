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
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CreateNamespaceCommand extends Command {
  private Option createNamespaceOptCopyConfig;
  private Option createNamesapceOptExcludeParentProps;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, TableExistsException,
      TableNotFoundException, IOException, ClassNotFoundException, NamespaceExistsException,
      NamespaceNotFoundException {

    // exclude parent properties; only valid with copy config
    if (cl.hasOption(createNamesapceOptExcludeParentProps.getLongOpt())
        && !cl.hasOption(createNamespaceOptCopyConfig.getOpt())) {
      throw new IllegalArgumentException(createNamesapceOptExcludeParentProps.getLongOpt()
          + " only valid when using " + createNamespaceOptCopyConfig.getLongOpt());
    }

    String namespace = cl.getArgs()[0];

    shellState.getAccumuloClient().namespaceOperations().create(namespace);
    if (!shellState.getAccumuloClient().namespaceOperations().exists(namespace)) {
      throw new IllegalArgumentException("Could not create namespace `" + namespace + "`");
    }

    // Copy configuration options if flag was set
    if (cl.hasOption(createNamespaceOptCopyConfig.getOpt())) {
      String srcNs = cl.getOptionValue(createNamespaceOptCopyConfig.getOpt());
      if (!srcNs.isEmpty() && !shellState.getAccumuloClient().namespaceOperations().exists(srcNs)) {
        throw new NamespaceNotFoundException(null, srcNs, null);
      }
      Map<String,String> userProps;
      if (cl.hasOption(createNamesapceOptExcludeParentProps.getLongOpt())) {
        // use namespace props - excludes parents in configuration
        userProps =
            shellState.getAccumuloClient().namespaceOperations().getNamespaceProperties(srcNs);
      } else {
        // use namespace config - include parents in configuration hierarchy
        userProps = shellState.getAccumuloClient().namespaceOperations().getConfiguration(srcNs);
      }
      if (userProps != null) {
        final Map<String,String> props = userProps;
        shellState.getAccumuloClient().namespaceOperations().modifyProperties(namespace,
            properties -> props.entrySet().stream()
                .filter(entry -> Property.isValidTablePropertyKey(entry.getKey()))
                .forEach(entry -> properties.put(entry.getKey(), entry.getValue())));
      }
    }
    return 0;
  }

  @Override
  public String description() {
    return "creates a new namespace";
  }

  @Override
  public String usage() {
    return getName() + " <namespaceName>";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    createNamespaceOptCopyConfig =
        new Option("cc", "copy-config", true, "namespace to copy configuration from");
    createNamespaceOptCopyConfig.setArgName("namespace");

    createNamesapceOptExcludeParentProps = new Option(null, "exclude-parent-properties", false,
        "exclude properties from its parent(s) when copying configuration");

    o.addOption(createNamespaceOptCopyConfig);
    o.addOption(createNamesapceOptExcludeParentProps);

    return o;
  }

  @Override
  public int numArgs() {
    return 1;
  }
}
