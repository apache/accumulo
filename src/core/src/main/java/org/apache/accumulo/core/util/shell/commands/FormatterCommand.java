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

import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class FormatterCommand extends Command {
  private Option resetOption, formatterClassOption, listClassOption;
  
  @Override
  public String description() {
    return "specifies a formatter to use for displaying database entries";
  }
  
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    if (cl.hasOption(resetOption.getOpt()))
      shellState.setFormatterClass(DefaultFormatter.class);
    else if (cl.hasOption(formatterClassOption.getOpt()))
      shellState.setFormatterClass(AccumuloClassLoader.loadClass(cl.getOptionValue(formatterClassOption.getOpt()), Formatter.class));
    else if (cl.hasOption(listClassOption.getOpt()))
      shellState.getReader().printString(shellState.getFormatterClass().getName() + "\n");
    return 0;
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    OptionGroup formatGroup = new OptionGroup();
    
    resetOption = new Option("r", "reset", false, "reset to default formatter");
    formatterClassOption = new Option("f", "formatter", true, "fully qualified name of formatter class to use");
    formatterClassOption.setArgName("className");
    listClassOption = new Option("l", "list", false, "display the current formatter");
    formatGroup.addOption(resetOption);
    formatGroup.addOption(formatterClassOption);
    formatGroup.addOption(listClassOption);
    formatGroup.setRequired(true);
    
    o.addOptionGroup(formatGroup);
    
    return o;
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
  
}