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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import jline.ConsoleReader;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class ConfigCommand extends Command {
  private Option tableOpt, deleteOpt, setOpt, filterOpt, disablePaginationOpt;
  
  private int COL1 = 8, COL2 = 7;
  private ConsoleReader reader;
  
  @Override
  public void registerCompletion(Token root, Map<Command.CompletionSet,Set<String>> completionSet) {
    Token cmd = new Token(getName());
    Token sub = new Token("-" + setOpt.getOpt());
    for (Property p : Property.values()) {
      if (!(p.getKey().endsWith(".")))
        sub.addSubcommand(new Token(p.toString()));
    }
    cmd.addSubcommand(sub);
    root.addSubcommand(cmd);
  }
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      IOException, ClassNotFoundException {
    reader = shellState.getReader();
    
    String tableName = cl.getOptionValue(tableOpt.getOpt());
    if (tableName != null && !shellState.getConnector().tableOperations().exists(tableName))
      throw new TableNotFoundException(null, tableName, null);
    
    if (cl.hasOption(deleteOpt.getOpt())) {
      // delete property from table
      String property = cl.getOptionValue(deleteOpt.getOpt());
      if (property.contains("="))
        throw new BadArgumentException("Invalid '=' operator in delete operation.", fullCommand, fullCommand.indexOf('='));
      if (tableName != null) {
        if (!Property.isValidTablePropertyKey(property))
          Shell.log.warn("Invalid per-table property : " + property + ", still removing from zookeeper if it's there.");
        
        shellState.getConnector().tableOperations().removeProperty(tableName, property);
        Shell.log.debug("Successfully deleted table configuration option.");
      } else {
        if (!Property.isValidZooPropertyKey(property))
          Shell.log.warn("Invalid per-table property : " + property + ", still removing from zookeeper if it's there.");
        shellState.getConnector().instanceOperations().removeProperty(property);
        Shell.log.debug("Successfully deleted system configuration option");
      }
    } else if (cl.hasOption(setOpt.getOpt())) {
      // set property on table
      String property = cl.getOptionValue(setOpt.getOpt()), value = null;
      if (!property.contains("="))
        throw new BadArgumentException("Missing '=' operator in set operation.", fullCommand, fullCommand.indexOf(property));
      
      String pair[] = property.split("=", 2);
      property = pair[0];
      value = pair[1];
      
      if (tableName != null) {
        if (!Property.isValidTablePropertyKey(property))
          throw new BadArgumentException("Invalid per-table property.", fullCommand, fullCommand.indexOf(property));
        
        if (property.equals(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey()))
          new ColumnVisibility(value); // validate that it is a valid expression
          
        shellState.getConnector().tableOperations().setProperty(tableName, property, value);
        Shell.log.debug("Successfully set table configuration option.");
      } else {
        if (!Property.isValidZooPropertyKey(property))
          throw new BadArgumentException("Property cannot be modified in zookeeper", fullCommand, fullCommand.indexOf(property));
        
        shellState.getConnector().instanceOperations().setProperty(property, value);
        Shell.log.debug("Successfully set system configuration option");
      }
    } else {
      // display properties
      TreeMap<String,String> systemConfig = new TreeMap<String,String>();
      systemConfig.putAll(shellState.getConnector().instanceOperations().getSystemConfiguration());
      
      TreeMap<String,String> siteConfig = new TreeMap<String,String>();
      siteConfig.putAll(shellState.getConnector().instanceOperations().getSiteConfiguration());
      
      TreeMap<String,String> defaults = new TreeMap<String,String>();
      for (Entry<String,String> defaultEntry : AccumuloConfiguration.getDefaultConfiguration())
        defaults.put(defaultEntry.getKey(), defaultEntry.getValue());
      
      Iterable<Entry<String,String>> acuconf = shellState.getConnector().instanceOperations().getSystemConfiguration().entrySet();
      if (tableName != null)
        acuconf = shellState.getConnector().tableOperations().getProperties(tableName);
      
      TreeMap<String,String> sortedConf = new TreeMap<String,String>();
      for (Entry<String,String> propEntry : acuconf) {
        sortedConf.put(propEntry.getKey(), propEntry.getValue());
      }
      
      for (Entry<String,String> propEntry : acuconf) {
        String key = propEntry.getKey();
        // only show properties with similar names to that
        // specified, or all of them if none specified
        if (cl.hasOption(filterOpt.getOpt()) && !key.contains(cl.getOptionValue(filterOpt.getOpt())))
          continue;
        if (tableName != null && !Property.isValidTablePropertyKey(key))
          continue;
        COL2 = Math.max(COL2, propEntry.getKey().length() + 3);
      }
      
      ArrayList<String> output = new ArrayList<String>();
      printConfHeader(output);
      
      for (Entry<String,String> propEntry : sortedConf.entrySet()) {
        String key = propEntry.getKey();
        
        // only show properties with similar names to that
        // specified, or all of them if none specified
        if (cl.hasOption(filterOpt.getOpt()) && !key.contains(cl.getOptionValue(filterOpt.getOpt())))
          continue;
        
        if (tableName != null && !Property.isValidTablePropertyKey(key))
          continue;
        
        String siteVal = siteConfig.get(key);
        String sysVal = systemConfig.get(key);
        String curVal = propEntry.getValue();
        String dfault = defaults.get(key);
        boolean printed = false;
        
        if (dfault != null && key.toLowerCase().contains("password")) {
          dfault = curVal = curVal.replaceAll(".", "*");
        }
        if (sysVal != null) {
          if (defaults.containsKey(key)) {
            printConfLine(output, "default", key, dfault);
            printed = true;
          }
          if (!defaults.containsKey(key) || !defaults.get(key).equals(siteVal)) {
            printConfLine(output, "site", printed ? "   @override" : key, siteVal == null ? "" : siteVal);
            printed = true;
          }
          if (!siteConfig.containsKey(key) || !siteVal.equals(sysVal)) {
            printConfLine(output, "system", printed ? "   @override" : key, sysVal == null ? "" : sysVal);
            printed = true;
          }
        }
        
        // show per-table value only if it is different (overridden)
        if (tableName != null && !curVal.equals(sysVal))
          printConfLine(output, "table", printed ? "   @override" : key, curVal);
      }
      printConfFooter(output);
      shellState.printLines(output.iterator(), !cl.hasOption(disablePaginationOpt.getOpt()));
    }
    return 0;
  }
  
  private void printConfHeader(ArrayList<String> output) {
    printConfFooter(output);
    output.add(String.format("%-" + COL1 + "s | %-" + COL2 + "s | %s", "SCOPE", "NAME", "VALUE"));
    printConfFooter(output);
  }
  
  private void printConfLine(ArrayList<String> output, String s1, String s2, String s3) {
    if (s2.length() < COL2)
      s2 += " " + Shell.repeat(".", COL2 - s2.length() - 1);
    output.add(String.format("%-" + COL1 + "s | %-" + COL2 + "s | %s", s1, s2,
        s3.replace("\n", "\n" + Shell.repeat(" ", COL1 + 1) + "|" + Shell.repeat(" ", COL2 + 2) + "|" + " ")));
  }
  
  private void printConfFooter(ArrayList<String> output) {
    int col3 = Math.max(1, Math.min(Integer.MAX_VALUE, reader.getTermwidth() - COL1 - COL2 - 6));
    output.add(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%-" + col3 + "s", Shell.repeat("-", COL1), Shell.repeat("-", COL2), Shell.repeat("-", col3)));
  }
  
  @Override
  public String description() {
    return "prints system properties and table specific properties";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    OptionGroup og = new OptionGroup();
    
    tableOpt = new Option(Shell.tableOption, "table", true, "table to display/set/delete properties for");
    deleteOpt = new Option("d", "delete", true, "delete a per-table property");
    setOpt = new Option("s", "set", true, "set a per-table property");
    filterOpt = new Option("f", "filter", true, "show only properties that contain this string");
    disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
    
    tableOpt.setArgName("table");
    deleteOpt.setArgName("property");
    setOpt.setArgName("property=value");
    filterOpt.setArgName("string");
    
    og.addOption(deleteOpt);
    og.addOption(setOpt);
    og.addOption(filterOpt);
    
    o.addOption(tableOpt);
    o.addOptionGroup(og);
    o.addOption(disablePaginationOpt);
    
    return o;
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
}
