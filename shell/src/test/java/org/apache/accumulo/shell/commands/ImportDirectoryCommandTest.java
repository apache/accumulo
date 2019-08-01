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
package org.apache.accumulo.shell.commands;

import static org.easymock.EasyMock.expectLastCall;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class ImportDirectoryCommandTest {

  private ImportDirectoryCommand cmd;

  private Connector conn;
  private CommandLine cli;
  private Shell shellState;
  private TableOperations tableOperations;

  @Before
  public void setup() {
    cmd = new ImportDirectoryCommand();

    // Initialize that internal state
    cmd.getOptions();

    conn = EasyMock.createMock(Connector.class);
    cli = EasyMock.createMock(CommandLine.class);
    shellState = EasyMock.createMock(Shell.class);
    tableOperations = EasyMock.createMock(TableOperations.class);

  }

  /**
   * Test original command form - no -t tablename option provided.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void testOriginalCmdForm() throws Exception {

    String[] cliArgs = {"in_dir", "fail_dir", "false"};
    //
    // EasyMock.expect(cli.hasOption('t')).andReturn(false);

    EasyMock.expect(cli.hasOption("t")).andReturn(false);

    EasyMock.expect(cli.getArgs()).andReturn(cliArgs);
    EasyMock.expect(cli.getArgs()).andReturn(cliArgs);
    EasyMock.expect(cli.getArgs()).andReturn(cliArgs);

    EasyMock.expect(shellState.getConnector()).andReturn(conn);
    EasyMock.expect(shellState.getTableName()).andReturn("tablename");

    shellState.checkTableState();
    expectLastCall().andVoid();

    // Table exists
    EasyMock.expect(conn.tableOperations()).andReturn(tableOperations);

    tableOperations.importDirectory("tablename", "in_dir", "fail_dir", false);
    expectLastCall().times(3);

    EasyMock.replay(conn, cli, shellState, tableOperations);

    cmd.execute("importdirectory in_dir fail_dir false", cli, shellState);

  }

  /**
   * Test with -t tablename option provided.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void testPassTableOptCmdForm() throws Exception {

    String[] cliArgs = {"in_dir", "fail_dir", "false"};
    //
    // EasyMock.expect(cli.hasOption('t')).andReturn(false);

    EasyMock.expect(cli.hasOption("t")).andReturn(true);
    EasyMock.expect(cli.hasOption("t")).andReturn(true);
    EasyMock.expect(cli.getOptionValue("t")).andReturn("passedName");

    EasyMock.expect(tableOperations.exists("passedName")).andReturn(true);
    EasyMock.expect(shellState.getConnector()).andReturn(conn);
    EasyMock.expect(conn.tableOperations()).andReturn(tableOperations);

    EasyMock.expect(cli.getArgs()).andReturn(cliArgs);
    EasyMock.expect(cli.getArgs()).andReturn(cliArgs);
    EasyMock.expect(cli.getArgs()).andReturn(cliArgs);

    EasyMock.expect(shellState.getConnector()).andReturn(conn);

    shellState.checkTableState();
    expectLastCall().andVoid();

    // Table exists
    EasyMock.expect(conn.tableOperations()).andReturn(tableOperations);

    tableOperations.importDirectory("passedName", "in_dir", "fail_dir", false);
    expectLastCall().times(3);

    EasyMock.replay(conn, cli, shellState, tableOperations);

    cmd.execute("importdirectory in_dir fail_dir false", cli, shellState);

  }
}
