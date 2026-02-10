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
package org.apache.accumulo.test.shell;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.TimeZone;

import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.shell.MockShell.TestShell;
import org.apache.accumulo.test.shell.ShellIT.StringInputStream;
import org.apache.accumulo.test.shell.ShellIT.TestOutputStream;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ShellAuthenticatorIT_SimpleSuite extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private StringInputStream input;
  private TestOutputStream output;
  private TestShell shell;
  private LineReader reader;
  private Terminal terminal;

  @BeforeEach
  public void setupShell() throws IOException {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    output = new TestOutputStream();
    input = new StringInputStream();
    terminal = new DumbTerminal(input, output);
    terminal.setSize(new Size(80, 24));
    reader = LineReaderBuilder.builder().terminal(terminal).build();
  }

  @AfterEach
  public void tearDownShell() {
    if (shell != null) {
      shell.shutdown();
    }
  }

  @Test
  public void testClientPropertiesFile() throws Exception {
    shell = new TestShell(reader);
    shell.setLogErrorsToConsole();
    shell.execute(new String[] {"-u", getAdminPrincipal(), "-p", getRootPassword(), "--config-file",
        getCluster().getClientPropsPath()});
    assertTrue(shell.getExitCode() == 0);
  }

  @Test
  public void testClientProperties() throws Exception {
    shell = new TestShell(reader);
    shell.setLogErrorsToConsole();
    shell.execute(new String[] {"-u", getAdminPrincipal(), "-p", getRootPassword(), "--config-file",
        getCluster().getClientPropsPath()});
    assertTrue(shell.getExitCode() == 0);
  }

  @Test
  public void testClientPropertiesBadPassword() throws Exception {
    shell = new TestShell(reader);
    shell.setLogErrorsToConsole();
    shell.execute(new String[] {"-u", getAdminPrincipal(), "-p", "BADPW", "--config-file",
        getCluster().getClientPropsPath()});
    assertTrue(shell.getExitCode() != 0);
  }

  @Test
  public void testAuthTimeoutPropertiesFile() throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    output = new TestOutputStream();
    input = new StringInputStream();
    terminal = new DumbTerminal(input, output);
    terminal.setSize(new Size(80, 24));
    reader = LineReaderBuilder.builder().terminal(terminal).build();
    shell = new TestShell(reader);
    shell.setLogErrorsToConsole();
    shell.execute(new String[] {"-u", getAdminPrincipal(), "-p", getRootPassword(),
        "--auth-timeout", "1", "--config-file", getCluster().getClientPropsPath()});
    assertTrue(shell.getExitCode() == 0);
    Thread.sleep(90000);
    shell.execCommand("whoami", false, false);
    assertTrue(output.get().contains("root"));
  }

}
