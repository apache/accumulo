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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp.Capability;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class HistoryCommandTest {

  HistoryCommand command;
  CommandLine cl;

  ByteArrayOutputStream baos;
  LineReader reader;
  Terminal terminal;
  Shell shell;

  @Before
  public void setUp() throws Exception {
    command = new HistoryCommand();
    command.getOptions(); // Make sure everything is initialized

    cl = createMock(CommandLine.class);
    expect(cl.hasOption("c")).andReturn(false);
    expect(cl.hasOption("np")).andReturn(true);
    replay(cl);

    History history = new DefaultHistory();
    history.add("foo");
    history.add("bar");

    baos = new ByteArrayOutputStream();

    String input = String.format("!1%n"); // Construct a platform dependent new-line
    terminal =
        TerminalBuilder.builder().streams(new ByteArrayInputStream(input.getBytes()), baos).build();
    reader = LineReaderBuilder.builder().history(history).terminal(terminal).build();

    shell = new Shell(reader);
  }

  @Test
  public void testCorrectNumbering() throws IOException {
    command.execute("", cl, shell);
    terminal.writer().flush();

    assertTrue(baos.toString().contains("2: bar"));
  }

  @Test
  public void testEventExpansion() throws IOException {
    // If we use an unsupported terminal, then history expansion doesn't work because JLine can't do
    // magic buffer manipulations.
    // This has been observed to be the case on certain versions of Eclipse. However, mvn is usually
    // fine.

    // Find better way to confirm terminal is supported. Maybe its not necessary anymore. not sure
    Assume.assumeTrue(reader.getTerminal().getBooleanCapability(Capability.clear_screen));

    reader.readLine();

    assertTrue(baos.toString().trim().endsWith("foo"));
  }

}
