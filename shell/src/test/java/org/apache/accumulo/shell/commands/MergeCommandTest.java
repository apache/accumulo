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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class MergeCommandTest {

  public static class TestMergeCommand extends MergeCommand {
    @Override
    int executeMerge(Shell shellState, String tableName, Text startRow, Text endRow, long size,
        boolean verbose, boolean force)
        throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
      if (size > 0) {
        shellState.getAccumuloClient().tableOperations().merge(tableName, startRow, endRow);
      }
      return 0;
    }
  }

  @Test
  public void testBeginRowHelp() {
    assertTrue(
        new MergeCommand().getOptions().getOption("b").getDescription().contains("(exclusive)"),
        "-b should say it is exclusive");
  }

  @Test
  public void mockMetadataMergeTest() throws Exception {
    MergeCommand cmd = new TestMergeCommand();

    AccumuloClient client = EasyMock.createMock(AccumuloClient.class);
    ClientContext context = EasyMock.createMock(ClientContext.class);
    TableOperations tableOps = EasyMock.createMock(TableOperations.class);
    InstanceOperations instOps = EasyMock.createMock(InstanceOperations.class);
    Shell shellState = EasyMock.createMock(Shell.class);

    Options opts = cmd.getOptions();

    CommandLineParser parser = new DefaultParser();
    String[] args = {"-t", MetadataTable.NAME, "-s", "0"};
    CommandLine cli = parser.parse(opts, args);

    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client).anyTimes();
    EasyMock.expect(shellState.getContext()).andReturn(context).anyTimes();
    EasyMock.expect(shellState.isVerbose()).andReturn(false).anyTimes();
    EasyMock.expect(client.tableOperations()).andReturn(tableOps).anyTimes();
    EasyMock.expect(tableOps.exists(MetadataTable.NAME)).andReturn(true).anyTimes();
    EasyMock.expect(shellState.confirm(
        " Warning!!! Merging the accumulo.metadata table incorrectly can result in system instability. Are you REALLY sure you want to merge?!?!?!"))
        .andReturn(Optional.of(true)).once();

    EasyMock.replay(client, context, tableOps, instOps, shellState);
    cmd.execute("merge", cli, shellState);
    EasyMock.verify(client, context, tableOps, instOps, shellState);
  }

  @Test
  public void mockMergeAllTabletsTest() throws Exception {
    MergeCommand cmd = new TestMergeCommand();

    AccumuloClient client = EasyMock.createMock(AccumuloClient.class);
    ClientContext context = EasyMock.createMock(ClientContext.class);
    TableOperations tableOps = EasyMock.createMock(TableOperations.class);
    InstanceOperations instOps = EasyMock.createMock(InstanceOperations.class);
    Shell shellState = EasyMock.createMock(Shell.class);

    Options opts = cmd.getOptions();

    CommandLineParser parser = new DefaultParser();
    String[] args = {"-t", "testTable"};
    CommandLine cli = parser.parse(opts, args);

    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client).anyTimes();
    EasyMock.expect(shellState.getContext()).andReturn(context).anyTimes();
    EasyMock.expect(shellState.isVerbose()).andReturn(false).anyTimes();
    EasyMock.expect(client.tableOperations()).andReturn(tableOps).anyTimes();
    EasyMock.expect(tableOps.exists("testTable")).andReturn(true).anyTimes();
    EasyMock.expect(shellState.confirm(
        " Warning!!! Are you REALLY sure you want to merge the entire table { testTable } into one tablet?!?!?!"))
        .andReturn(Optional.of(true)).once();

    EasyMock.replay(client, context, tableOps, instOps, shellState);
    cmd.execute("merge", cli, shellState);
    EasyMock.verify(client, context, tableOps, instOps, shellState);
  }
}
