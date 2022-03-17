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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.easymock.EasyMock;
import org.jline.reader.LineReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeleteAuthsCommandTest {

  private DeleteAuthsCommand cmd;

  @BeforeEach
  public void setup() {
    cmd = new DeleteAuthsCommand();

    // Initialize that internal state
    cmd.getOptions();
  }

  @Test
  public void deleteExistingAuth() throws Exception {
    AccumuloClient client = EasyMock.createMock(AccumuloClient.class);
    CommandLine cli = EasyMock.createMock(CommandLine.class);
    Shell shellState = EasyMock.createMock(Shell.class);
    LineReader reader = EasyMock.createMock(LineReader.class);
    SecurityOperations secOps = EasyMock.createMock(SecurityOperations.class);

    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client);

    // We're the root user
    EasyMock.expect(client.whoami()).andReturn("root");
    EasyMock.expect(cli.getOptionValue("u", "root")).andReturn("foo");
    EasyMock.expect(cli.getOptionValue("s")).andReturn("abc");

    EasyMock.expect(client.securityOperations()).andReturn(secOps);
    EasyMock.expect(client.securityOperations()).andReturn(secOps);
    EasyMock.expect(secOps.getUserAuthorizations("foo"))
        .andReturn(new Authorizations("abc", "123"));
    secOps.changeUserAuthorizations("foo", new Authorizations("123"));
    EasyMock.expectLastCall();

    EasyMock.replay(client, cli, shellState, reader, secOps);

    cmd.execute("deleteauths -u foo -s abc", cli, shellState);

    EasyMock.verify(client, cli, shellState, reader, secOps);
  }

  @Test
  public void deleteNonExistingAuth() throws Exception {
    AccumuloClient client = EasyMock.createMock(AccumuloClient.class);
    CommandLine cli = EasyMock.createMock(CommandLine.class);
    Shell shellState = EasyMock.createMock(Shell.class);
    LineReader reader = EasyMock.createMock(LineReader.class);
    SecurityOperations secOps = EasyMock.createMock(SecurityOperations.class);

    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client);

    // We're the root user
    EasyMock.expect(client.whoami()).andReturn("root");
    EasyMock.expect(cli.getOptionValue("u", "root")).andReturn("foo");
    EasyMock.expect(cli.getOptionValue("s")).andReturn("def");

    EasyMock.expect(client.securityOperations()).andReturn(secOps);
    EasyMock.expect(client.securityOperations()).andReturn(secOps);
    EasyMock.expect(secOps.getUserAuthorizations("foo"))
        .andReturn(new Authorizations("abc", "123"));
    secOps.changeUserAuthorizations("foo", new Authorizations("abc", "123"));
    EasyMock.expectLastCall();

    EasyMock.replay(client, cli, shellState, reader, secOps);

    cmd.execute("deleteauths -u foo -s def", cli, shellState);

    EasyMock.verify(client, cli, shellState, reader, secOps);
  }

  @Test
  public void deleteAllAuth() throws Exception {
    AccumuloClient client = EasyMock.createMock(AccumuloClient.class);
    CommandLine cli = EasyMock.createMock(CommandLine.class);
    Shell shellState = EasyMock.createMock(Shell.class);
    LineReader reader = EasyMock.createMock(LineReader.class);
    SecurityOperations secOps = EasyMock.createMock(SecurityOperations.class);

    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client);

    // We're the root user
    EasyMock.expect(client.whoami()).andReturn("root");
    EasyMock.expect(cli.getOptionValue("u", "root")).andReturn("foo");
    EasyMock.expect(cli.getOptionValue("s")).andReturn("abc,123");

    EasyMock.expect(client.securityOperations()).andReturn(secOps);
    EasyMock.expect(client.securityOperations()).andReturn(secOps);
    EasyMock.expect(secOps.getUserAuthorizations("foo"))
        .andReturn(new Authorizations("abc", "123"));
    secOps.changeUserAuthorizations("foo", new Authorizations());
    EasyMock.expectLastCall();

    EasyMock.replay(client, cli, shellState, reader, secOps);

    cmd.execute("deleteauths -u foo -s abc,123", cli, shellState);

    EasyMock.verify(client, cli, shellState, reader, secOps);
  }

}
