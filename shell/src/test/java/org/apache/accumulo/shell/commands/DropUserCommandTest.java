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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.easymock.EasyMock;
import org.jline.reader.LineReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DropUserCommandTest {

  private DropUserCommand cmd;

  @BeforeEach
  public void setup() {
    cmd = new DropUserCommand();

    // Initialize that internal state
    cmd.getOptions();
  }

  @Test
  public void dropUserWithoutForcePrompts() throws Exception {
    AccumuloClient client = EasyMock.createMock(AccumuloClient.class);
    CommandLine cli = EasyMock.createMock(CommandLine.class);
    Shell shellState = EasyMock.createMock(Shell.class);
    LineReader reader = EasyMock.createMock(LineReader.class);
    PrintWriter pw = EasyMock.createMock(PrintWriter.class);
    SecurityOperations secOps = EasyMock.createMock(SecurityOperations.class);

    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client);

    // The user we want to remove
    EasyMock.expect(cli.getArgs()).andReturn(new String[] {"user"});

    // We're the root user
    EasyMock.expect(client.whoami()).andReturn("root");

    // Force option was not provided
    EasyMock.expect(cli.hasOption("f")).andReturn(false);
    EasyMock.expect(shellState.getReader()).andReturn(reader);
    EasyMock.expect(shellState.getWriter()).andReturn(pw);
    pw.flush();
    EasyMock.expectLastCall().once();

    // Fake a "yes" response
    EasyMock.expect(reader.readLine(EasyMock.anyObject(String.class))).andReturn("yes");
    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client);

    EasyMock.expect(client.securityOperations()).andReturn(secOps);
    secOps.dropLocalUser("user");
    EasyMock.expectLastCall();

    EasyMock.replay(client, cli, shellState, reader, secOps);

    cmd.execute("dropuser foo -f", cli, shellState);

    EasyMock.verify(client, cli, shellState, reader, secOps);
  }

}
