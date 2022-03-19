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
package org.apache.accumulo.shell.format;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.FormatterConfig;
import org.apache.accumulo.shell.Shell;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeleterFormatterTest {
  DeleterFormatter formatter;
  Map<Key,Value> data;
  BatchWriter writer;
  BatchWriter exceptionWriter;
  Shell shellState;
  LineReader reader;
  Terminal terminal;
  PrintWriter pw;

  ByteArrayOutputStream baos;

  SettableInputStream input;

  class SettableInputStream extends InputStream {
    ByteArrayInputStream bais;

    @Override
    public int read() throws IOException {
      return bais.read();
    }

    public void set(String in) {
      bais = new ByteArrayInputStream(in.getBytes(UTF_8));
    }
  }

  @BeforeEach
  public void setUp() throws IOException, MutationsRejectedException {
    input = new SettableInputStream();
    baos = new ByteArrayOutputStream();

    MutationsRejectedException mre = createMock(MutationsRejectedException.class);

    writer = createNiceMock(BatchWriter.class);
    exceptionWriter = createNiceMock(BatchWriter.class);
    exceptionWriter.close();
    expectLastCall().andThrow(mre);
    exceptionWriter.addMutation(anyObject(Mutation.class));
    expectLastCall().andThrow(mre);

    shellState = createNiceMock(Shell.class);

    terminal = new DumbTerminal(input, baos);
    terminal.setSize(new Size(80, 24));
    reader = LineReaderBuilder.builder().terminal(terminal).build();
    pw = terminal.writer();

    expect(shellState.getReader()).andReturn(reader).anyTimes();
    expect(shellState.getWriter()).andReturn(pw).anyTimes();

    replay(writer, exceptionWriter, shellState);

    data = new TreeMap<>();
    data.put(new Key("r", "cf", "cq"), new Value("value"));
  }

  @Test
  public void testEmpty() {
    formatter = new DeleterFormatter(writer, Collections.<Key,Value>emptyMap().entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, true);
    assertFalse(formatter.hasNext());
  }

  @Test
  public void testSingle() throws IOException {
    formatter = new DeleterFormatter(writer, data.entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, true);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());

    verify("[DELETED]", " r ", "cf", "cq", "value");
  }

  @Test
  public void testNo() throws IOException {
    input.set("no\n");
    data.put(new Key("z"), new Value("v2"));
    formatter = new DeleterFormatter(writer, data.entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, false);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());

    verify("[SKIPPED]", " r ", "cf", "cq", "value");

    assertTrue(formatter.hasNext());
  }

  @Test
  public void testNoConfirmation() throws IOException {
    input.set("");
    data.put(new Key("z"), new Value("v2"));
    formatter = new DeleterFormatter(writer, data.entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, false);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());

    verify("[SKIPPED]", " r ", "cf", "cq", "value");

    assertFalse(formatter.hasNext());
  }

  @Test
  public void testYes() throws IOException {
    input.set("y\nyes\n");
    data.put(new Key("z"), new Value("v2"));
    formatter = new DeleterFormatter(writer, data.entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, false);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());
    verify("[DELETED]", " r ", "cf", "cq", "value");

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());
    verify("[DELETED]", " z ", "v2");
  }

  @Test
  public void testMutationException() {
    formatter = new DeleterFormatter(exceptionWriter, data.entrySet(),
        new FormatterConfig().setPrintTimestamps(true), shellState, true);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());
    assertFalse(formatter.hasNext());
  }

  private void verify(String... chunks) throws IOException {
    reader.getTerminal().writer().flush();

    String output = baos.toString();
    for (String chunk : chunks) {
      assertTrue(output.contains(chunk));
    }
  }
}
