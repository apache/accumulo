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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class ListTabletsCommandTest {

  final static String tableName = ListTabletsCommandTest.class.getName() + "-aTable";
  private static final TableId tableId = TableId.of("123");

  private static class TestListTabletsCommand extends ListTabletsCommand {
    @Override
    protected void printResults(CommandLine cl, Shell shellState, List<String> lines) {
      // there are three rows of tablet info plus 2 lines of header info
      assertEquals(lines.size(), 5, "Incorrect number of rows: " + lines.size());
      assertEquals(TabletInformation.header, lines.get(0));
      assertTrue(lines.get(1).startsWith("TABLE:"));
      assertTrue(lines.get(1).contains(tableName));

      // first table info
      List<String> items = new ArrayList<>(Arrays.asList(lines.get(2).split("\\s+")));
      assertTrue(lines.get(2).startsWith("1"));
      assertEquals("1", items.get(0));
      assertEquals("t-dir1", items.get(1));
      assertEquals("1", items.get(2));
      assertEquals("0", items.get(3));
      assertEquals("1,154", items.get(4));
      assertEquals("8,104", items.get(5));
      assertEquals("UNASSIGNED", items.get(6));
      assertEquals("None", items.get(7));
      assertEquals("123", items.get(8));
      assertEquals("-INF", items.get(9));
      assertEquals("d", items.get(10));
      assertEquals("ONDEMAND", items.get(11));
      // second tablet info
      items.clear();
      items = new ArrayList<>(Arrays.asList(lines.get(3).split("\\s+")));
      assertTrue(lines.get(3).startsWith("2"));
      assertEquals("2", items.get(0));
      assertEquals("t-dir2", items.get(1));
      assertEquals("2", items.get(2));
      assertEquals("1", items.get(3));
      assertEquals("1,243", items.get(4));
      assertEquals("13,204", items.get(5));
      assertEquals("HOSTED", items.get(6));
      assertEquals("localhost:1234", items.get(7));
      assertEquals("123", items.get(8));
      assertEquals("e", items.get(9));
      assertEquals("k", items.get(10));
      assertEquals("ALWAYS", items.get(11));
      // third tablet info
      items.clear();
      items = new ArrayList<>(Arrays.asList(lines.get(4).split("\\s+")));
      assertTrue(lines.get(4).startsWith("3"));
      assertEquals("3", items.get(0));
      assertEquals("t-dir3", items.get(1));
      assertEquals("3", items.get(2));
      assertEquals("2", items.get(3));
      assertEquals("3,223", items.get(4));
      assertEquals("81,204", items.get(5));
      assertEquals("UNASSIGNED", items.get(6));
      assertEquals("None", items.get(7));
      assertEquals("123", items.get(8));
      assertEquals("l", items.get(9));
      assertEquals("+INF", items.get(10));
      assertEquals("NEVER", items.get(11));
    }
  }

  @Test
  public void mockTest() throws Exception {
    ListTabletsCommand cmd = new TestListTabletsCommand();

    AccumuloClient client = EasyMock.createMock(AccumuloClient.class);
    ClientContext context = EasyMock.createMock(ClientContext.class);
    TableOperations tableOps = EasyMock.createMock(TableOperations.class);
    InstanceOperations instOps = EasyMock.createMock(InstanceOperations.class);
    Shell shellState = EasyMock.createMock(Shell.class);

    Options opts = cmd.getOptions();

    CommandLineParser parser = new DefaultParser();
    String[] args = {"-t", tableName};
    CommandLine cli = parser.parse(opts, args);

    TabletInformation[] tabletInformation = new TabletInformation[3];
    tabletInformation[0] = new TabletInformation(tableName, tableId, "d", null, 1, 0, 1154, 8104,
        "UNASSIGNED", "None", "t-dir1", TabletHostingGoal.ONDEMAND);
    tabletInformation[1] = new TabletInformation(tableName, tableId, "k", "e", 2, 1, 1243, 13204,
        "HOSTED", "localhost:1234", "t-dir2", TabletHostingGoal.ALWAYS);
    tabletInformation[2] = new TabletInformation(tableName, tableId, null, "l", 3, 2, 3223, 81204,
        "UNASSIGNED", "None", "t-dir3", TabletHostingGoal.NEVER);

    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client).anyTimes();
    EasyMock.expect(shellState.getContext()).andReturn(context).anyTimes();
    EasyMock.expect(client.tableOperations()).andReturn(tableOps).anyTimes();
    EasyMock.expect(context.tableOperations()).andReturn(tableOps).anyTimes();
    EasyMock.expect(tableOps.getTabletInformation(tableName, new Range()))
        .andReturn(Stream.of(tabletInformation));

    Map<String,String> idMap = new TreeMap<>();
    idMap.put(tableName, tableId.canonical());
    EasyMock.expect(tableOps.tableIdMap()).andReturn(idMap);

    EasyMock.replay(client, context, tableOps, instOps, shellState);
    cmd.execute("listTablets -t " + tableName, cli, shellState);
    EasyMock.verify(client, context, tableOps, instOps, shellState);
  }

}
