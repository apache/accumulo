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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.jline.reader.Expander;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.DefaultExpander;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.ExternalTerminal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HistoryCommandTest {

  HistoryCommand command;
  CommandLine cl;

  ByteArrayOutputStream baos;
  LineReader reader;
  Terminal terminal;
  Shell shell;

  @BeforeEach
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
    terminal = new ExternalTerminal("shell", "ansi", new ByteArrayInputStream(input.getBytes()),
        baos, UTF_8);
    reader = LineReaderBuilder.builder().history(history).terminal(terminal).build();

    shell = new Shell(reader);
  }

  @Test
  public void testCorrectNumbering() throws IOException {
    command.execute("", cl, shell);
    terminal.writer().flush();
    assertTrue(baos.toString().contains("2: bar"),
        "History order is not correct: " + baos.toString());
  }

  @Test
  public void testEventExpansion() {
    // If we use an unsupported terminal, then history expansion doesn't work because JLine can't do
    // magic buffer manipulations.
    // This has been observed to be the case on certain versions of Eclipse. However, mvn is usually
    // fine.

    reader.unsetOpt(LineReader.Option.DISABLE_EVENT_EXPANSION);
    Expander expander = new DefaultExpander();
    // Fails github QA since that doesn't have terminal with event expansion. Adding this check
    assumeFalse(expander.expandHistory(reader.getHistory(), baos.toString().trim()).isEmpty());
    assertEquals("foo", expander.expandHistory(reader.getHistory(), baos.toString().trim()));
  }
}
