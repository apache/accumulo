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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListTabletsCommandTest {

  private static final Logger log = LoggerFactory.getLogger(ListTabletsCommandTest.class);
  final String tableName = ListTabletsCommandTest.class.getName() + "-aTable";
  private static final TableId tableId = TableId.of("123");
  private static final String rowString = "123;a 123;m 123<";
  private static final List<String> rows = new ArrayList<>(Arrays.asList(rowString.split(" ")));

  private class TestListTabletsCommand extends ListTabletsCommand {
    @Override
    protected void printResults(CommandLine cl, Shell shellState, List<String> lines) {
      log.debug("Command run successfully. Output below...");
      for (String line : lines)
        log.debug(line);
      assertEquals(TabletRowInfo.header, lines.get(0));
      assertTrue(lines.get(1).startsWith("TABLE:"));
      // first table info
      assertTrue(lines.get(2).startsWith("1"));
      assertTrue(lines.get(2).contains("t-dir1"));
      assertTrue(lines.get(2).contains("100"));
      assertTrue(lines.get(2).contains("-INF"));
      assertTrue(lines.get(2).endsWith("a                   "));
      // second tablet info
      assertTrue(lines.get(3).startsWith("2"));
      assertTrue(lines.get(3).contains("t-dir2"));
      assertTrue(lines.get(3).contains("200"));
      assertTrue(lines.get(3).contains("a"));
      assertTrue(lines.get(3).endsWith("m                   "));
      // third tablet info
      assertTrue(lines.get(4).startsWith("3"));
      assertTrue(lines.get(4).contains("t-dir3"));
      assertTrue(lines.get(4).contains("300"));
      assertTrue(lines.get(4).contains("m"));
      assertTrue(lines.get(4).endsWith("+INF                "));
    }

    @Override
    protected List<TabletRowInfo> getMetadataInfo(Shell shellState,
        ListTabletsCommand.TableInfo tableInfo) throws Exception {
      List<TabletRowInfo> tablets = new ArrayList<>();
      KeyExtent ke1 = new KeyExtent(tableId, new Text("a"), null);
      KeyExtent ke2 = new KeyExtent(tableId, new Text("m"), new Text("a"));
      KeyExtent ke3 = new KeyExtent(tableId, null, new Text("m"));
      TabletMetadata.Location loc =
          new TabletMetadata.Location("localhost", "", TabletMetadata.LocationType.CURRENT);
      ListTabletsCommand.TabletRowInfo.Factory factory =
          new ListTabletsCommand.TabletRowInfo.Factory(tableName, ke1).dir("t-dir1").numFiles(1)
              .numWalLogs(1).numEntries(1).size(100).status(TabletState.HOSTED.toString())
              .location(loc).tableExists(true);
      tablets.add(factory.build());
      factory = new ListTabletsCommand.TabletRowInfo.Factory(tableName, ke2).dir("t-dir2")
          .numFiles(2).numWalLogs(2).numEntries(2).size(200).status(TabletState.HOSTED.toString())
          .location(loc).tableExists(true);
      tablets.add(factory.build());
      factory = new ListTabletsCommand.TabletRowInfo.Factory(tableName, ke3).dir("t-dir3")
          .numFiles(3).numWalLogs(3).numEntries(3).size(300).status(TabletState.HOSTED.toString())
          .location(loc).tableExists(true);
      tablets.add(factory.build());
      return tablets;
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

    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client).anyTimes();
    EasyMock.expect(shellState.getContext()).andReturn(context).anyTimes();
    EasyMock.expect(client.tableOperations()).andReturn(tableOps).anyTimes();

    Map<String,String> idMap = new TreeMap<>();
    idMap.put(tableName, tableId.canonical());
    EasyMock.expect(tableOps.tableIdMap()).andReturn(idMap);

    assertEquals(rows.size(), 3, "Incorrect number of rows: " + rows);

    EasyMock.replay(client, context, tableOps, instOps, shellState);
    cmd.execute("listTablets -t " + tableName, cli, shellState);
    EasyMock.verify(client, context, tableOps, instOps, shellState);
  }

  @Test
  public void defaultBuilderTest() {
    TableId id = TableId.of("123");
    Text startRow = new Text("a");
    Text endRow = new Text("z");
    ListTabletsCommand.TabletRowInfo.Factory factory =
        new ListTabletsCommand.TabletRowInfo.Factory("aName", new KeyExtent(id, endRow, startRow));

    ListTabletsCommand.TabletRowInfo info = factory.build();

    assertEquals("aName", info.tableName);
    assertEquals(id, info.tableId);
    assertEquals("a z", info.getTablet());
    assertEquals(0, info.numFiles);
    assertEquals(0, info.numWalLogs);
    assertEquals(0, info.numEntries);
    assertEquals(0, info.size);
    assertEquals("", info.status);
    assertEquals("", info.location);
    assertFalse(info.tableExists);
  }

  @Test
  public void builderTest() {
    TableId id = TableId.of("123");
    Text startRow = new Text("a");
    Text endRow = new Text("z");
    KeyExtent ke = new KeyExtent(id, endRow, startRow);
    TabletMetadata.Location loc =
        new TabletMetadata.Location("localhost", "", TabletMetadata.LocationType.CURRENT);
    ListTabletsCommand.TabletRowInfo.Factory factory =
        new ListTabletsCommand.TabletRowInfo.Factory("aName", ke).numFiles(1).numWalLogs(2)
            .numEntries(3).size(4).status(TabletState.HOSTED.toString()).location(loc)
            .tableExists(true);

    ListTabletsCommand.TabletRowInfo info = factory.build();

    assertEquals("aName", info.tableName);
    assertEquals(1, info.numFiles);
    assertEquals(2, info.numWalLogs);
    assertEquals("3", info.getNumEntries(false));
    assertEquals(3, info.numEntries);
    assertEquals("4", info.getSize(false));
    assertEquals(4, info.size);
    assertEquals("HOSTED", info.status);
    assertEquals("CURRENT:localhost", info.location);
    assertEquals(TableId.of("123"), info.tableId);
    assertEquals(startRow + " " + endRow, info.getTablet());
    assertTrue(info.tableExists);
  }
}
