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
package org.apache.accumulo.shell.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.io.Writer;

import org.apache.commons.io.output.WriterOutputStream;

import org.apache.commons.cli.CommandLine;
import jline.console.ConsoleReader;

import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.ShellOptionsJC;

/**
 * An Accumulo Shell implementation that allows a developer to attach an InputStream and Writer to the Shell for testing purposes.
 */
public class MockShell extends Shell {
  private static final String NEWLINE = "\n";
  
  protected InputStream in;
  protected OutputStream out;

  /**
   * Will only be set if you use either the Writer constructor or the setWriter(Writer) method
   * @deprecated since 1.6.0; use out
   */
  @Deprecated
  protected Writer writer = null;

  public MockShell(InputStream in, OutputStream out) throws IOException {
    super();
    this.in = in;
    this.out = out;
    // we presume they don't use the writer field unless they use the other constructor.
  }

  /**
   * @deprecated since 1.6.0; use OutputStream version
   */
  @Deprecated
  public MockShell(InputStream in, Writer out) throws IOException {
    this(in, new WriterOutputStream(out, Constants.UTF8.name()));
    this.writer = out;
  }
  
  public boolean config(String... args) {
    configError = super.config(args);
    
    // Update the ConsoleReader with the input and output "redirected"
    try {
      this.reader = new ConsoleReader(in, out);
    } catch (Exception e) {
      printException(e);
      configError = true;
    }
    
    // Don't need this for testing purposes
    this.reader.setHistoryEnabled(false);
    this.reader.setPaginationEnabled(false);
    
    // Make the parsing from the client easier;
    this.verbose = false;
    return configError;
  }
  
  @Override
  protected void setInstance(ShellOptionsJC options) {
    // We always want a MockInstance for this test
    instance = new MockInstance();
  }

  /**
   * @deprecated since 1.6.0; use ShellOptionsJC version
   */
  @Deprecated
  protected void setInstance(CommandLine cl) {
    // same result as in previous version
    setInstance((ShellOptionsJC)null);
  }
  
  public int start() throws IOException {
    if (configError)
      return 1;
    
    String input;
    if (isVerbose())
      printInfo();
    
    if (execFile != null) {
      java.util.Scanner scanner = new java.util.Scanner(execFile, StandardCharsets.UTF_8.name());
      try {
        while (scanner.hasNextLine() && !hasExited()) {
          execCommand(scanner.nextLine(), true, isVerbose());
        }
      } finally {
        scanner.close();
      }
    } else if (execCommand != null) {
      for (String command : execCommand.split("\n")) {
        execCommand(command, true, isVerbose());
      }
      return exitCode;
    }
    
    while (true) {
      if (hasExited())
        return exitCode;
      
      reader.setPrompt(getDefaultPrompt());
      input = reader.readLine();
      if (input == null) {
        reader.println();
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
   * @param out
   *          the output stream to set
   */
  public void setConsoleWriter(OutputStream out) {
    this.out = out;
  }

  /**
   * @deprecated since 1.6.0; use the OutputStream version
   */
  @Deprecated
  public void setConsoleWriter(Writer out) {
    setConsoleWriter(new WriterOutputStream(out, Constants.UTF8.name()));
    this.writer = out;
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
    
    return new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));
  }
}
