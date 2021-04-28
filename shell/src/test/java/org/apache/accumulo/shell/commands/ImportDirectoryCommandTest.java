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
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.bulk.BulkImport;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ImportDirectoryCommandTest {

  private ImportDirectoryCommand cmd;

  private AccumuloClient client;
  private CommandLine cli;
  private Shell shellState;
  private TableOperations tableOperations;
  private BulkImport bulkImport;

  @Before
  public void setup() {
    cmd = new ImportDirectoryCommand();

    // Initialize that internal state
    cmd.getOptions();

    client = createMock(AccumuloClient.class);
    cli = createMock(CommandLine.class);
    shellState = createMock(Shell.class);
    tableOperations = createMock(TableOperations.class);
    bulkImport = createMock(BulkImport.class);
  }

  @After
  public void verifyMocks() {
    verify(client, cli, shellState, tableOperations, bulkImport);
  }

  /**
   * Test original command form - no -t tablename option provided.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void testOriginalCmdForm() throws Exception {
    String[] cliArgs = {"in_dir", "false"};

    // no -t option, use current table context
    expect(cli.hasOption("t")).andReturn(false).once();
    // no -i option supplied
    expect(cli.hasOption("i")).andReturn(false).once();
    expect(shellState.getTableName()).andReturn("tablename").once();

    expect(cli.getArgs()).andReturn(cliArgs).atLeastOnce();
    expect(shellState.getAccumuloClient()).andReturn(client).atLeastOnce();
    expect(client.tableOperations()).andReturn(tableOperations);

    shellState.checkTableState();
    expectLastCall().once();

    // given the -i option, the ignoreEmptyBulkDir boolean is set to false
    expect(tableOperations.importDirectory("in_dir", false)).andReturn(bulkImport).once();
    expect(bulkImport.to("tablename")).andReturn(bulkImport).once();
    expect(bulkImport.tableTime(false)).andReturn(bulkImport).once();
    bulkImport.load();
    expectLastCall().once();

    replay(client, cli, shellState, tableOperations, bulkImport);
    cmd.execute("importdirectory in_dir false", cli, shellState);
  }

  /**
   * Test with -t tablename option provided.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void testPassTableOptCmdForm() throws Exception {
    String[] cliArgs = {"in_dir", "false"};

    // -i option specified, ignore empty bulk import directory
    expect(cli.hasOption("i")).andReturn(true).once();
    // -t option specified, table is from option
    expect(cli.hasOption("t")).andReturn(true).once();
    expect(cli.getOptionValue("t")).andReturn("passedName").once();
    expect(tableOperations.exists("passedName")).andReturn(true).once();

    expect(cli.getArgs()).andReturn(cliArgs).atLeastOnce();
    expect(shellState.getAccumuloClient()).andReturn(client).atLeastOnce();
    expect(client.tableOperations()).andReturn(tableOperations).atLeastOnce();

    // shellState.checkTableState() is NOT called

    // given the -i option, the ignoreEmptyBulkDir boolean is set to true
    expect(tableOperations.importDirectory("in_dir", true)).andReturn(bulkImport).once();
    expect(bulkImport.to("passedName")).andReturn(bulkImport).once();
    expect(bulkImport.tableTime(false)).andReturn(bulkImport).once();
    bulkImport.load();
    expectLastCall().once();

    replay(client, cli, shellState, tableOperations, bulkImport);
    cmd.execute("importdirectory in_dir false", cli, shellState);
  }
}
