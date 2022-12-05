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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.shell.Shell;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockShell {
  private static final Logger shellLog = LoggerFactory.getLogger(MockShell.class);
  private static final ErrorMessageCallback noop = new ErrorMessageCallback();

  public TestOutputStream output;
  public StringInputStream input;
  public Shell shell;
  public LineReader reader;
  public Terminal terminal;

  MockShell(String user, String rootPass, String instanceName, String zookeepers, File configFile)
      throws IOException {
    ClientInfo info = ClientInfo.from(configFile.toPath());
    // start the shell
    output = new TestOutputStream();
    input = new StringInputStream();
    terminal = new DumbTerminal(input, output);
    terminal.setSize(new Size(80, 24));
    reader = LineReaderBuilder.builder().terminal(terminal).build();
    shell = new Shell(reader);
    shell.setLogErrorsToConsole();
    if (info.saslEnabled()) {
      // Pull the kerberos principal out when we're using SASL
      shell.config("-u", user, "-z", instanceName, zookeepers, "--config-file",
          configFile.getAbsolutePath());
    } else {
      shell.config("-u", user, "-p", rootPass, "-z", instanceName, zookeepers, "--config-file",
          configFile.getAbsolutePath());
    }
    exec("quit", true);
    shell.start();
    shell.setExit(false);
  }

  String exec(String cmd) throws IOException {
    output.clear();
    shell.execCommand(cmd, true, true);
    return output.get();
  }

  String exec(String cmd, boolean expectGoodExit) throws IOException {
    return exec(cmd, expectGoodExit, noop);
  }

  String exec(String cmd, boolean expectGoodExit, ErrorMessageCallback callback)
      throws IOException {
    String result = exec(cmd);
    if (expectGoodExit) {
      assertGoodExit("", true, callback);
    } else {
      assertBadExit("", true, callback);
    }
    return result;
  }

  String exec(String cmd, boolean expectGoodExit, String expectString) throws IOException {
    return exec(cmd, expectGoodExit, expectString, noop);
  }

  String exec(String cmd, boolean expectGoodExit, String expectString,
      ErrorMessageCallback callback) throws IOException {
    return exec(cmd, expectGoodExit, expectString, true, callback);
  }

  String exec(String cmd, boolean expectGoodExit, String expectString, boolean stringPresent)
      throws IOException {
    return exec(cmd, expectGoodExit, expectString, stringPresent, noop);
  }

  String exec(String cmd, boolean expectGoodExit, String expectString, boolean stringPresent,
      ErrorMessageCallback callback) throws IOException {
    String result = exec(cmd);
    if (expectGoodExit) {
      assertGoodExit(expectString, stringPresent, callback);
    } else {
      assertBadExit(expectString, stringPresent, callback);
    }
    return result;
  }

  void assertGoodExit(String s, boolean stringPresent) {
    assertGoodExit(s, stringPresent, noop);
  }

  void assertGoodExit(String s, boolean stringPresent, ErrorMessageCallback callback) {
    shellLog.debug("Shell Output: '{}'", output.get());
    if (shell.getExitCode() != 0) {
      String errorMsg = callback.getErrorMessage();
      assertEquals(0, shell.getExitCode(), errorMsg);
    }

    if (!s.isEmpty()) {
      assertEquals(stringPresent, output.get().contains(s),
          s + " present in " + output.get() + " was not " + stringPresent);
    }
  }

  void assertBadExit(String s, boolean stringPresent, ErrorMessageCallback callback) {
    shellLog.debug(output.get());
    if (shell.getExitCode() == 0) {
      String errorMsg = callback.getErrorMessage();
      assertTrue(shell.getExitCode() > 0, errorMsg);
    }

    if (!s.isEmpty()) {
      assertEquals(stringPresent, output.get().contains(s),
          s + " present in " + output.get() + " was not " + stringPresent);
    }
    shell.resetExitCode();
  }

  void writeToHistory(String cmd) {
    input.set(cmd);
    reader.readLine();
  }

  static class TestOutputStream extends OutputStream {
    StringBuilder sb = new StringBuilder();

    @Override
    public void write(int b) {
      sb.append((char) (0xff & b));
    }

    public String get() {
      return sb.toString();
    }

    public void clear() {
      sb.setLength(0);
    }
  }

  static class StringInputStream extends InputStream {
    private String source = "";
    private int offset = 0;

    @Override
    public int read() {
      if (offset == source.length()) {
        return '\n';
      } else {
        return source.charAt(offset++);
      }
    }

    public void set(String other) {
      source = other;
      offset = 0;
    }
  }
}
