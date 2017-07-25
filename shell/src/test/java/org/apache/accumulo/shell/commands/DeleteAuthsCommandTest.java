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

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import jline.console.ConsoleReader;

/**
 *
 */
public class DeleteAuthsCommandTest {

  private DeleteAuthsCommand cmd;

  @Before
  public void setup() {
    cmd = new DeleteAuthsCommand();

    // Initialize that internal state
    cmd.getOptions();
  }

  @Test
  public void deleteExistingAuth() throws Exception {
    Connector conn = EasyMock.createMock(Connector.class);
    CommandLine cli = EasyMock.createMock(CommandLine.class);
    Shell shellState = EasyMock.createMock(Shell.class);
    ConsoleReader reader = EasyMock.createMock(ConsoleReader.class);
    SecurityOperations secOps = EasyMock.createMock(SecurityOperations.class);

    EasyMock.expect(shellState.getConnector()).andReturn(conn);

    // We're the root user
    EasyMock.expect(conn.whoami()).andReturn("root");
    EasyMock.expect(cli.getOptionValue("u", "root")).andReturn("foo");
    EasyMock.expect(cli.getOptionValue("s")).andReturn("abc");

    EasyMock.expect(conn.securityOperations()).andReturn(secOps);
    EasyMock.expect(conn.securityOperations()).andReturn(secOps);
    EasyMock.expect(secOps.getUserAuthorizations("foo")).andReturn(new Authorizations("abc", "123"));
    secOps.changeUserAuthorizations("foo", new Authorizations("123"));
    EasyMock.expectLastCall();

    EasyMock.replay(conn, cli, shellState, reader, secOps);

    cmd.execute("deleteauths -u foo -s abc", cli, shellState);

    EasyMock.verify(conn, cli, shellState, reader, secOps);
  }

  @Test
  public void deleteNonExistingAuth() throws Exception {
    Connector conn = EasyMock.createMock(Connector.class);
    CommandLine cli = EasyMock.createMock(CommandLine.class);
    Shell shellState = EasyMock.createMock(Shell.class);
    ConsoleReader reader = EasyMock.createMock(ConsoleReader.class);
    SecurityOperations secOps = EasyMock.createMock(SecurityOperations.class);

    EasyMock.expect(shellState.getConnector()).andReturn(conn);

    // We're the root user
    EasyMock.expect(conn.whoami()).andReturn("root");
    EasyMock.expect(cli.getOptionValue("u", "root")).andReturn("foo");
    EasyMock.expect(cli.getOptionValue("s")).andReturn("def");

    EasyMock.expect(conn.securityOperations()).andReturn(secOps);
    EasyMock.expect(conn.securityOperations()).andReturn(secOps);
    EasyMock.expect(secOps.getUserAuthorizations("foo")).andReturn(new Authorizations("abc", "123"));
    secOps.changeUserAuthorizations("foo", new Authorizations("abc", "123"));
    EasyMock.expectLastCall();

    EasyMock.replay(conn, cli, shellState, reader, secOps);

    cmd.execute("deleteauths -u foo -s def", cli, shellState);

    EasyMock.verify(conn, cli, shellState, reader, secOps);
  }

  @Test
  public void deleteAllAuth() throws Exception {
    Connector conn = EasyMock.createMock(Connector.class);
    CommandLine cli = EasyMock.createMock(CommandLine.class);
    Shell shellState = EasyMock.createMock(Shell.class);
    ConsoleReader reader = EasyMock.createMock(ConsoleReader.class);
    SecurityOperations secOps = EasyMock.createMock(SecurityOperations.class);

    EasyMock.expect(shellState.getConnector()).andReturn(conn);

    // We're the root user
    EasyMock.expect(conn.whoami()).andReturn("root");
    EasyMock.expect(cli.getOptionValue("u", "root")).andReturn("foo");
    EasyMock.expect(cli.getOptionValue("s")).andReturn("abc,123");

    EasyMock.expect(conn.securityOperations()).andReturn(secOps);
    EasyMock.expect(conn.securityOperations()).andReturn(secOps);
    EasyMock.expect(secOps.getUserAuthorizations("foo")).andReturn(new Authorizations("abc", "123"));
    secOps.changeUserAuthorizations("foo", new Authorizations());
    EasyMock.expectLastCall();

    EasyMock.replay(conn, cli, shellState, reader, secOps);

    cmd.execute("deleteauths -u foo -s abc,123", cli, shellState);

    EasyMock.verify(conn, cli, shellState, reader, secOps);
  }

}
