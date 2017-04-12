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
package org.apache.accumulo.shell.format;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import org.junit.Before;
import org.junit.Test;

import jline.UnsupportedTerminal;
import jline.console.ConsoleReader;

public class DeleterFormatterTest {
  DeleterFormatter formatter;
  Map<Key,Value> data;
  BatchWriter writer;
  BatchWriter exceptionWriter;
  Shell shellState;

  ByteArrayOutputStream baos;
  ConsoleReader reader;

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

  @Before
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

    reader = new ConsoleReader(input, baos, new UnsupportedTerminal());
    expect(shellState.getReader()).andReturn(reader).anyTimes();

    replay(writer, exceptionWriter, shellState);

    data = new TreeMap<>();
    data.put(new Key("r", "cf", "cq"), new Value("value".getBytes(UTF_8)));
  }

  @Test
  public void testEmpty() {
    formatter = new DeleterFormatter(writer, Collections.<Key,Value> emptyMap().entrySet(), new FormatterConfig().setPrintTimestamps(true), shellState, true);
    assertFalse(formatter.hasNext());
  }

  @Test
  public void testSingle() throws IOException {
    formatter = new DeleterFormatter(writer, data.entrySet(), new FormatterConfig().setPrintTimestamps(true), shellState, true);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());

    verify("[DELETED]", " r ", "cf", "cq", "value");
  }

  @Test
  public void testNo() throws IOException {
    input.set("no\n");
    data.put(new Key("z"), new Value("v2".getBytes(UTF_8)));
    formatter = new DeleterFormatter(writer, data.entrySet(), new FormatterConfig().setPrintTimestamps(true), shellState, false);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());

    verify("[SKIPPED]", " r ", "cf", "cq", "value");

    assertTrue(formatter.hasNext());
  }

  @Test
  public void testNoConfirmation() throws IOException {
    input.set("");
    data.put(new Key("z"), new Value("v2".getBytes(UTF_8)));
    formatter = new DeleterFormatter(writer, data.entrySet(), new FormatterConfig().setPrintTimestamps(true), shellState, false);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());

    verify("[SKIPPED]", " r ", "cf", "cq", "value");

    assertFalse(formatter.hasNext());
  }

  @Test
  public void testYes() throws IOException {
    input.set("y\nyes\n");
    data.put(new Key("z"), new Value("v2".getBytes(UTF_8)));
    formatter = new DeleterFormatter(writer, data.entrySet(), new FormatterConfig().setPrintTimestamps(true), shellState, false);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());
    verify("[DELETED]", " r ", "cf", "cq", "value");

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());
    verify("[DELETED]", " z ", "v2");
  }

  @Test
  public void testMutationException() {
    formatter = new DeleterFormatter(exceptionWriter, data.entrySet(), new FormatterConfig().setPrintTimestamps(true), shellState, true);

    assertTrue(formatter.hasNext());
    assertNull(formatter.next());
    assertFalse(formatter.hasNext());
  }

  private void verify(String... chunks) throws IOException {
    reader.flush();

    String output = baos.toString();
    for (String chunk : chunks) {
      assertTrue(output.contains(chunk));
    }
  }
}
