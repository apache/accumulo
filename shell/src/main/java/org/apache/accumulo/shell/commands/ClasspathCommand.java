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

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;

public class ClasspathCommand extends Command {

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) {

    final PrintWriter writer = shellState.getWriter();
    printClassPath(writer);
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

  public static void printClassPath(PrintWriter writer) {
    try {
      writer.print("Accumulo Shell Classpath: \n");

      final String javaClassPath = System.getProperty("java.class.path");
      if (javaClassPath == null) {
        throw new IllegalStateException("java.class.path is not set");
      }
      Arrays.stream(javaClassPath.split(File.pathSeparator)).forEach(classPathUri -> {
        writer.print(classPathUri + "\n");
      });

      writer.print("\n");
    } catch (Exception t) {
      throw new RuntimeException(t);
    }
  }
}
