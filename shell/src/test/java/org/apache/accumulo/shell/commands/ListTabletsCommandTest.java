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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletInformationImpl;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ListTabletsCommandTest {

  final static String tableName = ListTabletsCommandTest.class.getName() + "-aTable";

  private static class TestListTabletsCommand extends ListTabletsCommand {

    @Override
    protected void printResults(CommandLine cl, Shell shellState, List<String> lines) {

      // there are three rows of tablet info plus 2 lines of header info
      assertEquals(lines.size(), 5, "Incorrect number of rows: " + lines.size());
      assertEquals(header, lines.get(0));
      assertTrue(lines.get(1).startsWith("TABLE:"));
      assertTrue(lines.get(1).contains(tableName));

      // first table info
      List<String> items = new ArrayList<>(Arrays.asList(lines.get(2).split("\\s+")));
      assertTrue(lines.get(2).startsWith("1"));
      assertEquals("1", items.get(0)); // line count
      assertEquals("t-dir1", items.get(1)); // dir name
      assertEquals("2", items.get(2)); // files
      assertEquals("1", items.get(3)); // wals
      assertEquals("1,116", items.get(4)); // entries
      assertEquals("385,606", items.get(5)); // size
      assertEquals("HOSTED", items.get(6)); // status
      assertEquals("CURRENT:server1:8555", items.get(7)); // location
      assertEquals("123", items.get(8)); // id
      assertEquals("-INF", items.get(9)); // start
      assertEquals("d", items.get(10)); // end
      assertEquals("ONDEMAND", items.get(11)); // goal
      // second tablet info
      items.clear();
      items = new ArrayList<>(Arrays.asList(lines.get(3).split("\\s+")));
      assertTrue(lines.get(3).startsWith("2"));
      assertEquals("2", items.get(0));
      assertEquals("t-dir2", items.get(1));
      assertEquals("1", items.get(2));
      assertEquals("0", items.get(3));
      assertEquals("142", items.get(4));
      assertEquals("5,323", items.get(5));
      assertEquals("HOSTED", items.get(6));
      assertEquals("CURRENT:server2:2354", items.get(7));
      assertEquals("123", items.get(8));
      assertEquals("e", items.get(9));
      assertEquals("k", items.get(10));
      assertEquals("HOSTED", items.get(11));
      // third tablet info
      items.clear();
      items = new ArrayList<>(Arrays.asList(lines.get(4).split("\\s+")));
      assertTrue(lines.get(4).startsWith("3"));
      assertEquals("3", items.get(0));
      assertEquals("t-dir3", items.get(1));
      assertEquals("1", items.get(2));
      assertEquals("2", items.get(3));
      assertEquals("231", items.get(4));
      assertEquals("95,832", items.get(5));
      assertEquals("UNASSIGNED", items.get(6));
      assertEquals("None", items.get(7));
      assertEquals("123", items.get(8));
      assertEquals("l", items.get(9));
      assertEquals("+INF", items.get(10));
      assertEquals("UNHOSTED", items.get(11));
    }

  }

  @Test
  public void mockTest() throws Exception {
    ListTabletsCommand cmd = new TestListTabletsCommand();

    TableId tableId = TableId.of("123");

    TServerInstance ser1 = new TServerInstance(HostAndPort.fromParts("server1", 8555), "s001");
    TServerInstance ser2 = new TServerInstance(HostAndPort.fromParts("server2", 2354), "s002");

    StoredTabletFile sf11 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-dir1/sf11.rf")).insert();
    DataFileValue dfv11 = new DataFileValue(5643, 89);

    StoredTabletFile sf12 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-dir1/sf12.rf")).insert();
    DataFileValue dfv12 = new DataFileValue(379963, 1027);

    StoredTabletFile sf21 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-dir2/sf21.rf")).insert();
    DataFileValue dfv21 = new DataFileValue(5323, 142);

    StoredTabletFile sf31 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-dir3/sf31.rf")).insert();
    DataFileValue dfv31 = new DataFileValue(95832L, 231);

    KeyExtent extent = new KeyExtent(tableId, new Text("d"), null);

    LogEntry le1 = LogEntry.fromPath("localhost+8020/" + UUID.randomUUID());
    LogEntry le2 = LogEntry.fromPath("localhost+8020/" + UUID.randomUUID());

    TabletMetadata tm1 =
        TabletMetadata.builder(extent).putTabletAvailability(TabletAvailability.ONDEMAND)
            .putLocation(TabletMetadata.Location.current(ser1)).putFile(sf11, dfv11)
            .putFile(sf12, dfv12).putWal(le1).putDirName("t-dir1").build();

    extent = new KeyExtent(tableId, new Text("k"), new Text("e"));
    TabletMetadata tm2 =
        TabletMetadata.builder(extent).putTabletAvailability(TabletAvailability.HOSTED)
            .putLocation(TabletMetadata.Location.current(ser2)).putFile(sf21, dfv21)
            .putDirName("t-dir2").build(LOGS);

    extent = new KeyExtent(tableId, null, new Text("l"));
    TabletMetadata tm3 =
        TabletMetadata.builder(extent).putTabletAvailability(TabletAvailability.UNHOSTED)
            .putFile(sf31, dfv31).putWal(le1).putWal(le2).putDirName("t-dir3").build(LOCATION);

    TabletInformationImpl[] tabletInformation = new TabletInformationImpl[3];
    tabletInformation[0] = new TabletInformationImpl(tm1, "HOSTED");
    tabletInformation[1] = new TabletInformationImpl(tm2, "HOSTED");
    tabletInformation[2] = new TabletInformationImpl(tm3, "UNASSIGNED");

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
    EasyMock.expect(context.tableOperations()).andReturn(tableOps).anyTimes();
    EasyMock.expect(tableOps.getTabletInformation(tableName, new Range()))
        .andReturn(Stream.of(tabletInformation));

    Map<String,String> idMap = new TreeMap<>();
    idMap.put(tableName, tableId.canonical());
    EasyMock.expect(tableOps.tableIdMap()).andReturn(idMap);

    EasyMock.replay(client, context, tableOps, instOps, shellState);
    cmd.execute("listtablets -t " + tableName, cli, shellState);
    EasyMock.verify(client, context, tableOps, instOps, shellState);
  }

}
