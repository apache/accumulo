/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.io.UncheckedIOException;

import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;

import jline.console.ConsoleReader;

public class ClasspathCommand extends Command {
  @SuppressWarnings("deprecation")
  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) {
    final ConsoleReader reader = shellState.getReader();
    org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader.printClassPath(s -> {
      try {
        reader.print(s);
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }, true);
    return 0;
  }

  @Override
  public String description() {
    return "lists the current files on the classpath";
  }

  @Override
  public int numArgs() {
    return 0;
  }
}
