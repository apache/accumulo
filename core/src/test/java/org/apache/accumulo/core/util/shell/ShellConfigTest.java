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
package org.apache.accumulo.core.util.shell;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.io.PrintWriter;

import jline.console.ConsoleReader;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.shell.ShellTest.TestOutputStream;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.ParameterException;

public class ShellConfigTest {
  TestOutputStream output;
  Shell shell;
  PrintStream out;

  @Before
  public void setUp() throws Exception {
    Shell.log.setLevel(Level.ERROR);

    out = System.out;
    output = new TestOutputStream();
    System.setOut(new PrintStream(output));

    shell = new Shell(new ConsoleReader(new FileInputStream(FileDescriptor.in), output), new PrintWriter(output));
    shell.setLogErrorsToConsole();
  }

  @After
  public void teardown() throws Exception {
    shell.shutdown();
    output.clear();
    System.setOut(out);
  }

  @Test
  public void testHelp() {
    assertFalse(shell.config("--help"));
    assertTrue("Did not print usage", output.get().startsWith("Usage"));
  }

  @Test
  public void testBadArg() {
    assertFalse(shell.config("--bogus"));
    assertTrue("Did not print usage", output.get().startsWith("Usage"));
  }

  @Test
  public void testTokenWithoutOptions() {
    assertFalse(shell.config("--fake", "-tc", PasswordToken.class.getCanonicalName()));
    assertFalse(output.get().contains(ParameterException.class.getCanonicalName()));
  }

  @Test
  public void testTokenAndOption() {
    assertTrue(shell.config("--fake", "-tc", PasswordToken.class.getCanonicalName(), "-u", "foo", "-l", "password=foo"));
  }

  @Test
  public void testTokenAndOptionAndPassword() {
    assertFalse(shell.config("--fake", "-tc", PasswordToken.class.getCanonicalName(), "-l", "password=foo", "-p", "bar"));
    assertTrue(output.get().contains(ParameterException.class.getCanonicalName()));
  }
}
