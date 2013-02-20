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
package org.apache.accumulo.core.client.mock;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;

import jline.ConsoleReader;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.cli.CommandLine;

/**
 * An Accumulo Shell implementation that allows a developer to attach an InputStream and Writer to the Shell for testing purposes.
 */
public class MockShell extends Shell {
  private static final String NEWLINE = "\n";
  
  protected InputStream in;
  protected Writer writer;
  
  public MockShell(InputStream in, Writer writer) throws IOException {
    super();
    this.in = in;
    this.writer = writer;
  }
  
  public boolean config(String... args) {
    configError = super.config(args);
    
    // Update the ConsoleReader with the input and output "redirected"
    try {
      this.reader = new ConsoleReader(in, writer);
    } catch (Exception e) {
      printException(e);
      configError = true;
    }
    
    // Don't need this for testing purposes
    this.reader.setUseHistory(false);
    this.reader.setUsePagination(false);
    
    // Make the parsing from the client easier;
    this.verbose = false;
    return configError;
  }
  
  @Override
  protected void setInstance(CommandLine cl) {
    // We always want a MockInstance for this test
    instance = new MockInstance();
  }
  
  public int start() throws IOException {
    if (configError)
      return 1;
    
    String input;
    if (isVerbose())
      printInfo();
    
    if (execFile != null) {
      java.util.Scanner scanner = new java.util.Scanner(new File(execFile));
      while (scanner.hasNextLine())
        execCommand(scanner.nextLine(), true, isVerbose());
    } else if (execCommand != null) {
      for (String command : execCommand.split("\n")) {
        execCommand(command, true, isVerbose());
      }
      return exitCode;
    }
    
    while (true) {
      if (hasExited())
        return exitCode;
      
      reader.setDefaultPrompt(getDefaultPrompt());
      input = reader.readLine();
      if (input == null) {
        reader.printNewline();
        return exitCode;
      } // user canceled
      
      execCommand(input, false, false);
    }
  }
  
  /**
   * @param in
   *          the in to set
   */
  public void setConsoleInputStream(InputStream in) {
    this.in = in;
  }
  
  /**
   * @param writer
   *          the writer to set
   */
  public void setConsoleWriter(Writer writer) {
    this.writer = writer;
  }
  
  /**
   * Convenience method to create the byte-array to hand to the console
   * 
   * @param commands
   *          An array of commands to run
   * @return A byte[] input stream which can be handed to the console.
   */
  public static ByteArrayInputStream makeCommands(String... commands) {
    StringBuilder sb = new StringBuilder(commands.length * 8);
    
    for (String command : commands) {
      sb.append(command).append(NEWLINE);
    }
    
    return new ByteArrayInputStream(sb.toString().getBytes());
  }
}
