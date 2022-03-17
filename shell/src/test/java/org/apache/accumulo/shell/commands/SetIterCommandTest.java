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

import java.io.PrintWriter;
import java.util.EnumSet;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.easymock.EasyMock;
import org.jline.reader.LineReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SetIterCommandTest {
  private SetIterCommand cmd;

  @BeforeEach
  public void setup() {
    cmd = new SetIterCommand();

    // Initialize that internal state
    cmd.getOptions();
  }

  @Test
  public void addColumnAgeOffFilter() throws Exception {
    AccumuloClient client = EasyMock.createMock(AccumuloClient.class);
    CommandLine cli = EasyMock.createMock(CommandLine.class);
    Shell shellState = EasyMock.createMock(Shell.class);
    LineReader reader = EasyMock.createMock(LineReader.class);
    PrintWriter pw = EasyMock.createMock(PrintWriter.class);
    TableOperations tableOperations = EasyMock.createMock(TableOperations.class);

    // Command line parsing
    EasyMock.expect(cli.hasOption("all")).andReturn(true);
    EasyMock.expect(cli.hasOption("all")).andReturn(true);
    EasyMock.expect(cli.hasOption("all")).andReturn(true);
    EasyMock.expect(cli.hasOption("t")).andReturn(true);
    EasyMock.expect(cli.hasOption("t")).andReturn(true);
    EasyMock.expect(cli.getOptionValue("t")).andReturn("foo");
    EasyMock.expect(cli.hasOption("ns")).andReturn(false);
    EasyMock.expect(cli.getOptionValue("p")).andReturn("21");
    EasyMock.expect(cli.getOptionValue("class"))
        .andReturn("org.apache.accumulo.core.iterators.user.ColumnAgeOffFilter");
    EasyMock.expect(cli.hasOption("ageoff")).andReturn(false);
    EasyMock.expect(cli.hasOption("regex")).andReturn(false);
    EasyMock.expect(cli.hasOption("reqvis")).andReturn(false);
    EasyMock.expect(cli.hasOption("vers")).andReturn(false);
    EasyMock.expect(cli.getOptionValue("n", null)).andReturn(null);

    // Loading the class
    EasyMock.expect(shellState.getClassLoader(cli, shellState))
        .andReturn(ClassLoaderUtil.getClassLoader(null));

    // Parsing iterator options
    pw.flush();
    EasyMock.expectLastCall().times(3);

    pw.println(EasyMock.anyObject(String.class));
    EasyMock.expectLastCall().times(2);

    EasyMock.expect(shellState.getReader()).andReturn(reader);
    EasyMock.expect(shellState.getWriter()).andReturn(pw);

    // Shell asking for negate option, we pass in an empty string to pickup the default value of
    // 'false'
    EasyMock.expect(reader.readLine(EasyMock.anyObject(String.class))).andReturn("");

    // Shell asking for the unnamed option for the column (a:a) and the TTL (1)
    EasyMock.expect(reader.readLine(EasyMock.anyObject(String.class))).andReturn("a:a 1");

    // Shell asking for another unnamed option; we pass in an empty string to signal that we are
    // done adding options
    EasyMock.expect(reader.readLine(EasyMock.anyObject(String.class))).andReturn("");
    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client);

    // Table exists
    EasyMock.expect(client.tableOperations()).andReturn(tableOperations);
    EasyMock.expect(tableOperations.exists("foo")).andReturn(true);

    // Testing class load
    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client);
    EasyMock.expect(client.tableOperations()).andReturn(tableOperations);
    EasyMock.expect(tableOperations.testClassLoad("foo",
        "org.apache.accumulo.core.iterators.user.ColumnAgeOffFilter",
        SortedKeyValueIterator.class.getName())).andReturn(true);

    // Attach iterator
    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client);
    EasyMock.expect(client.tableOperations()).andReturn(tableOperations);
    tableOperations.attachIterator(EasyMock.eq("foo"), EasyMock.anyObject(IteratorSetting.class),
        EasyMock.eq(EnumSet.allOf(IteratorScope.class)));
    EasyMock.expectLastCall().once();

    EasyMock.expect(shellState.getTableName()).andReturn("foo").anyTimes();

    EasyMock.replay(client, cli, shellState, reader, tableOperations, pw);

    cmd.execute(
        "setiter -all -p 21 -t foo"
            + " -class org.apache.accumulo.core.iterators.user.ColumnAgeOffFilter",
        cli, shellState);

    EasyMock.verify(client, cli, shellState, reader, tableOperations, pw);
  }
}
