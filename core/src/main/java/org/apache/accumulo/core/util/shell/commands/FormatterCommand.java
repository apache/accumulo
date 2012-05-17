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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class FormatterCommand extends Command {
  private Option removeFormatterOption, formatterClassOption, listClassOption;
  
  @Override
  public String description() {
    return "specifies a formatter to use for displaying table entries";
  }
  
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    String tableName = OptUtil.getTableOpt(cl, shellState);
    
    if (cl.hasOption(removeFormatterOption.getOpt())) {
      // Remove the property
      shellState.getConnector().tableOperations().removeProperty(tableName, Property.TABLE_FORMATTER_CLASS.toString());
      
      shellState.getReader().printString("Removed formatter on " + tableName + "\n");
    } else if (cl.hasOption(listClassOption.getOpt())) {
      // Get the options for this table
      Iterator<Entry<String,String>> iter = shellState.getConnector().tableOperations().getProperties(tableName).iterator();
      
      while (iter.hasNext()) {
        Entry<String,String> ent = iter.next();
        
        // List all parameters with the property name
        if (ent.getKey().startsWith(Property.TABLE_FORMATTER_CLASS.toString())) {
          shellState.getReader().printString(ent.getKey() + ": " + ent.getValue() + "\n");
        }
      }
    } else {
      // Set the formatter with the provided options
      String className = cl.getOptionValue(formatterClassOption.getOpt());
      
      // Set the formatter property on the table
      shellState.getConnector().tableOperations().setProperty(tableName, Property.TABLE_FORMATTER_CLASS.toString(), className);
    }
    
    return 0;
  }
  
  public static Class<? extends Formatter> getCurrentFormatter(String tableName, Shell shellState) {
    Iterator<Entry<String,String>> props;
    try {
      props = shellState.getConnector().tableOperations().getProperties(tableName).iterator();
    } catch (AccumuloException e) {
      return null;
    } catch (TableNotFoundException e) {
      return null;
    }
    
    while (props.hasNext()) {
      Entry<String,String> ent = props.next();
      if (ent.getKey().equals(Property.TABLE_FORMATTER_CLASS.toString())) {
        Class<? extends Formatter> formatter;
        try {
          formatter = AccumuloClassLoader.loadClass(ent.getValue(), Formatter.class);
        } catch (ClassNotFoundException e) {
          return null;
        }
        
        return formatter;
      }
    }
    
    return null;
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    OptionGroup actionGroup = new OptionGroup();
    
    formatterClassOption = new Option("f", "formatter", true, "fully qualified name of the formatter class to use");
    formatterClassOption.setArgName("className");
    
    // Action to take: apply (default), remove, list
    removeFormatterOption = new Option("r", "remove", false, "remove the current formatter");
    listClassOption = new Option("l", "list", false, "display the current formatter");
    
    actionGroup.addOption(formatterClassOption);
    actionGroup.addOption(removeFormatterOption);
    actionGroup.addOption(listClassOption);
    actionGroup.setRequired(true);
    
    o.addOptionGroup(actionGroup);
    o.addOption(OptUtil.tableOpt("table to set the formatter on"));
    
    return o;
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
  
}
