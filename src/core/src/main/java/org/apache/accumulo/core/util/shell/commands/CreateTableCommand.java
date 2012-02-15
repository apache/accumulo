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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.conf.PerColumnIteratorConfig;
import org.apache.accumulo.core.security.VisibilityConstraint;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;

@SuppressWarnings("deprecation")
public class CreateTableCommand extends Command {
  private Option createTableOptCopySplits;
  private Option createTableOptCopyConfig;
  private Option createTableOptSplit;
  private Option createTableOptAgg;
  private Option createTableOptTimeLogical;
  private Option createTableOptTimeMillis;
  private Option createTableNoDefaultIters;
  private Option createTableOptEVC;
  private Option base64Opt;
  private Option createTableOptFormatter;
  public static String testTable;
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableExistsException,
      TableNotFoundException, IOException, ClassNotFoundException {
    
    String testTableName = cl.getArgs()[0];
    
    if (!testTableName.matches(Constants.VALID_TABLE_NAME_REGEX)) {
      shellState.getReader().printString("Only letters, numbers and underscores are allowed for use in table names. \n");
      throw new IllegalArgumentException();
    }
    
    String tableName = cl.getArgs()[0];
    if (shellState.getConnector().tableOperations().exists(tableName))
      throw new TableExistsException(null, tableName, null);
    
    SortedSet<Text> partitions = new TreeSet<Text>();
    List<PerColumnIteratorConfig> aggregators = new ArrayList<PerColumnIteratorConfig>();
    boolean decode = cl.hasOption(base64Opt.getOpt());
    
    if (cl.hasOption(createTableOptAgg.getOpt())) {
      Shell.log.warn("aggregators are deprecated");
      String agg = cl.getOptionValue(createTableOptAgg.getOpt());
      
      EscapeTokenizer st = new EscapeTokenizer(agg, "=,");
      if (st.count() % 2 != 0) {
        printHelp();
        return 0;
      }
      Iterator<String> iter = st.iterator();
      while (iter.hasNext()) {
        String col = iter.next();
        String className = iter.next();
        
        EscapeTokenizer colToks = new EscapeTokenizer(col, ":");
        Iterator<String> tokIter = colToks.iterator();
        Text cf = null, cq = null;
        if (colToks.count() < 1 || colToks.count() > 2)
          throw new BadArgumentException("column must be in the format cf[:cq]", fullCommand, fullCommand.indexOf(col));
        cf = new Text(tokIter.next());
        if (colToks.count() == 2)
          cq = new Text(tokIter.next());
        
        aggregators.add(new PerColumnIteratorConfig(cf, cq, className));
      }
    }
    if (cl.hasOption(createTableOptSplit.getOpt())) {
      String f = cl.getOptionValue(createTableOptSplit.getOpt());
      
      String line;
      java.util.Scanner file = new java.util.Scanner(new File(f));
      while (file.hasNextLine()) {
        line = file.nextLine();
        if (!line.isEmpty())
          partitions.add(decode ? new Text(Base64.decodeBase64(line.getBytes())) : new Text(line));
      }
    } else if (cl.hasOption(createTableOptCopySplits.getOpt())) {
      String oldTable = cl.getOptionValue(createTableOptCopySplits.getOpt());
      if (!shellState.getConnector().tableOperations().exists(oldTable))
        throw new TableNotFoundException(null, oldTable, null);
      partitions.addAll(shellState.getConnector().tableOperations().getSplits(oldTable));
    }
    
    if (cl.hasOption(createTableOptCopyConfig.getOpt())) {
      String oldTable = cl.getOptionValue(createTableOptCopyConfig.getOpt());
      if (!shellState.getConnector().tableOperations().exists(oldTable))
        throw new TableNotFoundException(null, oldTable, null);
    }
    
    TimeType timeType = TimeType.MILLIS;
    if (cl.hasOption(createTableOptTimeLogical.getOpt()))
      timeType = TimeType.LOGICAL;
    
    // create table
    shellState.getConnector().tableOperations().create(tableName, true, timeType);
    if (partitions.size() > 0)
      shellState.getConnector().tableOperations().addSplits(tableName, partitions);
    if (aggregators.size() > 0)
      shellState.getConnector().tableOperations().addAggregators(tableName, aggregators);
    
    shellState.setTableName(tableName); // switch shell to new table
    // context
    
    if (cl.hasOption(createTableNoDefaultIters.getOpt())) {
      for (String key : IteratorUtil.generateInitialTableProperties().keySet())
        shellState.getConnector().tableOperations().removeProperty(tableName, key);
    }
    
    // Copy options if flag was set
    if (cl.hasOption(createTableOptCopyConfig.getOpt())) {
      if (shellState.getConnector().tableOperations().exists(tableName)) {
        Iterable<Entry<String,String>> configuration = shellState.getConnector().tableOperations()
            .getProperties(cl.getOptionValue(createTableOptCopyConfig.getOpt()));
        for (Entry<String,String> entry : configuration) {
          if (Property.isValidTablePropertyKey(entry.getKey())) {
            shellState.getConnector().tableOperations().setProperty(tableName, entry.getKey(), entry.getValue());
          }
        }
      }
    }
    
    if (cl.hasOption(createTableOptEVC.getOpt())) {
      int max = 0;
      Iterable<Entry<String,String>> props = shellState.getConnector().tableOperations().getProperties(tableName);
      boolean vcSet = false;
      for (Entry<String,String> entry : props) {
        if (entry.getKey().startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey())) {
          int num = Integer.parseInt(entry.getKey().substring(Property.TABLE_CONSTRAINT_PREFIX.getKey().length()));
          if (num > max)
            max = num;
          
          if (entry.getValue().equals(VisibilityConstraint.class.getName()))
            vcSet = true;
        }
      }
      
      if (!vcSet)
        shellState.getConnector().tableOperations()
            .setProperty(tableName, Property.TABLE_CONSTRAINT_PREFIX.getKey() + (max + 1), VisibilityConstraint.class.getName());
    }
    
    // Load custom formatter if set
    if (cl.hasOption(createTableOptFormatter.getOpt())) {
      String formatterClass = cl.getOptionValue(createTableOptFormatter.getOpt());
      
      shellState.getConnector().tableOperations().setProperty(tableName, Property.TABLE_FORMATTER_CLASS.toString(), formatterClass);
    }
    
    return 0;
  }
  
  @Override
  public String description() {
    return "creates a new table, with optional aggregators and optionally pre-split";
  }
  
  @Override
  public String usage() {
    return getName() + " <tableName>";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    
    createTableOptCopyConfig = new Option("cc", "copy-config", true, "table to copy configuration from");
    createTableOptCopySplits = new Option("cs", "copy-splits", true, "table to copy current splits from");
    createTableOptSplit = new Option("sf", "splits-file", true, "file with newline separated list of rows to create a pre-split table");
    createTableOptAgg = new Option("a", "aggregator", true, "comma separated column=aggregator");
    createTableOptTimeLogical = new Option("tl", "time-logical", false, "use logical time");
    createTableOptTimeMillis = new Option("tm", "time-millis", false, "use time in milliseconds");
    createTableNoDefaultIters = new Option("ndi", "no-default-iterators", false, "prevents creation of the normal default iterator set");
    createTableOptEVC = new Option("evc", "enable-visibility-constraint", false,
        "prevents users from writing data they can not read.  When enabling this may want to consider disabling bulk import and alter table");
    createTableOptFormatter = new Option("f", "formatter", true, "default formatter to set");
    
    createTableOptCopyConfig.setArgName("table");
    createTableOptCopySplits.setArgName("table");
    createTableOptSplit.setArgName("filename");
    createTableOptAgg.setArgName("{<columnfamily>[:<columnqualifier>]=<aggregation class>}");
    createTableOptFormatter.setArgName("className");
    
    // Splits and CopySplits are put in an optionsgroup to make them
    // mutually exclusive
    OptionGroup splitOrCopySplit = new OptionGroup();
    splitOrCopySplit.addOption(createTableOptSplit);
    splitOrCopySplit.addOption(createTableOptCopySplits);
    
    OptionGroup timeGroup = new OptionGroup();
    timeGroup.addOption(createTableOptTimeLogical);
    timeGroup.addOption(createTableOptTimeMillis);
    
    base64Opt = new Option("b64", "base64encoded", false, "decode encoded split points");
    o.addOption(base64Opt);
    
    o.addOptionGroup(splitOrCopySplit);
    o.addOptionGroup(timeGroup);
    o.addOption(createTableOptSplit);
    o.addOption(createTableOptAgg);
    o.addOption(createTableOptCopyConfig);
    o.addOption(createTableNoDefaultIters);
    o.addOption(createTableOptEVC);
    o.addOption(createTableOptFormatter);
    
    return o;
  }
  
  @Override
  public int numArgs() {
    return 1;
  }
}
