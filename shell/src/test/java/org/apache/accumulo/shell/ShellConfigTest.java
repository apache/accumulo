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
package org.apache.accumulo.shell;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;

import jline.console.ConsoleReader;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.shell.ShellTest.TestOutputStream;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.ParameterException;

public class ShellConfigTest {
  TestOutputStream output;
  Shell shell;
  PrintStream out;
  File config;

  @Before
  public void setUp() throws Exception {
    Shell.log.setLevel(Level.ERROR);

    out = System.out;
    output = new TestOutputStream();
    System.setOut(new PrintStream(output));
    config = Files.createTempFile(null, null).toFile();

    shell = new Shell(new ConsoleReader(new FileInputStream(FileDescriptor.in), output), new PrintWriter(output));
    shell.setLogErrorsToConsole();
  }

  @After
  public void teardown() throws Exception {
    shell.shutdown();
    output.clear();
    System.setOut(out);
    if (config.exists()) {
      config.delete();
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
  public void testHelp() {
    assertFalse(shell.config(args("--help")));
    assertTrue("Did not print usage", output.get().startsWith("Usage"));
  }

  @Test
  public void testBadArg() {
    assertFalse(shell.config(args("--bogus")));
    assertTrue("Did not print usage", output.get().startsWith("Usage"));
  }

  @Test
  public void testTokenWithoutOptions() {
    assertFalse(shell.config(args("--fake", "-tc", PasswordToken.class.getCanonicalName())));
    assertFalse(output.get().contains(ParameterException.class.getCanonicalName()));
  }

  @Test
  public void testTokenAndOption() {
    assertTrue(shell.config(args("--fake", "-tc", PasswordToken.class.getCanonicalName(), "-u", "foo", "-l", "password=foo")));
  }

  @Test
  public void testTokenAndOptionAndPassword() {
    assertFalse(shell.config(args("--fake", "-tc", PasswordToken.class.getCanonicalName(), "-l", "password=foo", "-p", "bar")));
    assertTrue(output.get().contains(ParameterException.class.getCanonicalName()));
  }
}
