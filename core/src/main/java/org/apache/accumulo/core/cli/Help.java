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
package org.apache.accumulo.core.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class Help {
  @Parameter(names = {"-h", "-?", "--help", "-help"}, help = true)
  public boolean help = false;

  public void parseArgs(String programName, String[] args, Object... others) {
    JCommander commander = new JCommander();
    commander.addObject(this);
    for (Object other : others)
      commander.addObject(other);
    commander.setProgramName(programName);
    try {
      commander.parse(args);
    } catch (ParameterException ex) {
      commander.usage();
      exitWithError(ex.getMessage(), 1);
    }
    if (help) {
      commander.usage();
      exit(0);
    }
  }

  public void exit(int status) {
    System.exit(status);
  }

  public void exitWithError(String message, int status) {
    System.err.println(message);
    exit(status);
  }
}
