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
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNamespaceExistsException;
import org.apache.accumulo.core.client.TableNamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CreateNamespaceCommand extends Command {
  private Option createTableOptCopyConfig;
  private Option base64Opt;
  
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException,
      TableExistsException, TableNotFoundException, IOException, ClassNotFoundException, TableNamespaceExistsException, TableNamespaceNotFoundException {
    
    if (createTableOptCopyConfig == null) {
      getOptions();
    }
    
    String namespace = cl.getArgs()[0];
    
    shellState.getConnector().tableNamespaceOperations().create(namespace);
    
    // Copy options if flag was set
    if (cl.hasOption(createTableOptCopyConfig.getOpt())) {
      String copy = cl.getOptionValue(createTableOptCopyConfig.getOpt());
      if (shellState.getConnector().tableNamespaceOperations().exists(namespace)) {
        Iterable<Entry<String,String>> configuration = shellState.getConnector().tableNamespaceOperations().getProperties(copy);
        for (Entry<String,String> entry : configuration) {
          if (Property.isValidTablePropertyKey(entry.getKey())) {
            shellState.getConnector().tableNamespaceOperations().setProperty(namespace, entry.getKey(), entry.getValue());
          }
        }
      }
    }
    
    return 0;
  }
  
  @Override
  public String description() {
    return "creates a new table namespace";
  }
  
  @Override
  public String usage() {
    return getName() + " <namespaceName>";
  }
  
  @Override
  public Options getOptions() {
    final Options o = new Options();
    
    createTableOptCopyConfig = new Option("cc", "copy-config", true, "table namespace to copy configuration from");
    
    createTableOptCopyConfig.setArgName("tableNamespace");
    
    base64Opt = new Option("b64", "base64encoded", false, "decode encoded split points");
    o.addOption(base64Opt);
    o.addOption(createTableOptCopyConfig);
    
    return o;
  }
  
  @Override
  public int numArgs() {
    return 1;
  }
}
