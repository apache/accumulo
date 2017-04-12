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
package org.apache.accumulo.test.shell;

import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;

public class DebugCommand extends Command {

  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    Set<String> lines = new TreeSet<>();
    lines.add("This is a test");
    shellState.printLines(lines.iterator(), true);
    return 0;
  }

  @Override
  public String description() {
    return "prints a message to test extension feature";
  }

  @Override
  public int numArgs() {
    return 0;
  }

}
