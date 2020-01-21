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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.junit.After;
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

    conn = createMock(Connector.class);
    cli = createMock(CommandLine.class);
    shellState = createMock(Shell.class);
    tableOperations = createMock(TableOperations.class);
  }

  @After
  public void verifyMocks() {
    verify(conn, cli, shellState, tableOperations);
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

    // no -t option, use current table context
    expect(cli.hasOption("t")).andReturn(false).once();
    expect(shellState.getTableName()).andReturn("tablename").once();

    expect(cli.getArgs()).andReturn(cliArgs).atLeastOnce();
    expect(shellState.getConnector()).andReturn(conn).atLeastOnce();
    expect(conn.tableOperations()).andReturn(tableOperations);

    shellState.checkTableState();
    expectLastCall().once();

    tableOperations.importDirectory("tablename", "in_dir", "fail_dir", false);
    expectLastCall().once();

    replay(conn, cli, shellState, tableOperations);
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

    // -t option specified, table is from option
    expect(cli.hasOption("t")).andReturn(true).once();
    expect(cli.getOptionValue("t")).andReturn("passedName").once();
    expect(tableOperations.exists("passedName")).andReturn(true).once();

    expect(cli.getArgs()).andReturn(cliArgs).atLeastOnce();
    expect(shellState.getConnector()).andReturn(conn).atLeastOnce();
    expect(conn.tableOperations()).andReturn(tableOperations).atLeastOnce();

    // shellState.checkTableState() is NOT called

    tableOperations.importDirectory("passedName", "in_dir", "fail_dir", false);
    expectLastCall().once();

    replay(conn, cli, shellState, tableOperations);
    cmd.execute("importdirectory in_dir fail_dir false", cli, shellState);
  }
}
