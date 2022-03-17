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
package org.apache.accumulo.shell;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShellConfigTest {

  public static class TestOutputStream extends OutputStream {
    StringBuilder sb = new StringBuilder();

    @Override
    public void write(int b) throws IOException {
      sb.append((char) (0xff & b));
    }

    public String get() {
      return sb.toString();
    }

    public void clear() {
      sb.setLength(0);
    }
  }

  TestOutputStream output;
  Shell shell;
  PrintStream out;
  File config;
  private static final Logger log = LoggerFactory.getLogger(ShellConfigTest.class);

  @BeforeEach
  public void setUp() throws Exception {
    out = System.out;
    output = new TestOutputStream();
    System.setOut(new PrintStream(output));
    config = Files.createTempFile(null, null).toFile();
    Terminal terminal = new DumbTerminal(new FileInputStream(FileDescriptor.in), output);
    terminal.setSize(new Size(80, 24));
    LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();
    shell = new Shell(reader);
    shell.setLogErrorsToConsole();
  }

  @AfterEach
  public void teardown() throws Exception {
    shell.shutdown();
    output.clear();
    System.setOut(out);
    if (config.exists()) {
      if (!config.delete()) {
        log.error("Unable to delete {}", config);
      }
    }
  }

  public String[] args(String... args) {
    // Avoid a locally installed client configuration file from breaking the test
    String[] finalArgs = new String[args.length + 2];
    int i = 0;
    finalArgs[i++] = "--config-file";
    finalArgs[i++] = config.toString();
    for (String arg : args) {
      finalArgs[i++] = arg;
    }
    return finalArgs;
  }

  @Test
  public void testHelp() throws IOException {
    assertFalse(shell.config(args("--help")));
    assertTrue(output.get().startsWith("Usage"), "Did not print usage");
  }

  @Test
  public void testBadArg() throws IOException {
    assertFalse(shell.config(args("--bogus")));
    // JCommander versions after 1.60 will cause the Shell to detect the arg as Unrecognized option
    assertTrue(output.get().startsWith("ERROR"), "Did not print Error");
    assertTrue(output.get().contains("Usage"), "Did not print usage");
  }

}
