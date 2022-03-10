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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.zookeeper.ServiceLock.ServiceLockPath;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ReadOnlyRepo;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.easymock.EasyMock;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;
import org.junit.BeforeClass;
import org.junit.Test;

public class FateCommandTest {
  private static FateCommand cmd;
  private static Shell shellState;
  private static LineReader reader;
  private static Terminal terminal;
  private static PrintWriter pw;
  private static AccumuloClient client;
  private static InstanceOperations intOps;

  private static ByteArrayOutputStream baos;

  private static SettableInputStream input;

  static class SettableInputStream extends InputStream {
    ByteArrayInputStream bais;

    @Override
    public int read() throws IOException {
      return bais.read();
    }

    public void set(String in) {
      bais = new ByteArrayInputStream(in.getBytes(UTF_8));
    }
  }

  @BeforeClass
  public static void setup() throws IOException {
    cmd = new FateCommand();
    cmd.getOptions();
  }

  @Test
  public void testFailTx() throws Exception {
    client = createMock(AccumuloClient.class);
    CommandLine cli = createMock(CommandLine.class);
    shellState = createMock(Shell.class);
    reader = createMock(LineReader.class);
    pw = createMock(PrintWriter.class);
    intOps = createMock(InstanceOperations.class);
    expect(shellState.getAccumuloClient()).andReturn(client);
    String[] args = {"fail", "1234"};
    List<String> txids = new ArrayList<>();

    // The user we want to remove
    expect(cli.getArgs()).andReturn(args);
    expect(cli.getArgList()).andReturn(List.of(args));
    expect(List.of(args).subList(1, args.length)).andReturn(txids);

    expect(client.whoami()).andReturn("root");

    expect(shellState.getReader()).andReturn(reader);
    expect(shellState.getWriter()).andReturn(pw);
    pw.flush();
    EasyMock.expectLastCall().once();
    EasyMock.expect(shellState.getAccumuloClient()).andReturn(client);
    expect(client.instanceOperations()).andReturn(intOps);
    intOps.fateFail(txids);
    EasyMock.expectLastCall().once();

    replay(client, cli, shellState, reader, intOps);
    cmd.execute("fate fail 1234", cli, shellState);
    verify(client, cli, shellState, reader, intOps);
  }

  @Test
  public void testDump() {
    ZooStore<FateCommand> zs = createMock(ZooStore.class);
    ReadOnlyRepo<FateCommand> ser = createMock(ReadOnlyRepo.class);
    long tid1 = Long.parseLong("12345", 16);
    long tid2 = Long.parseLong("23456", 16);
    expect(zs.getStack(tid1)).andReturn(List.of(ser)).once();
    expect(zs.getStack(tid2)).andReturn(List.of(ser)).once();

    replay(zs);

    // FateCommand cmd = new FateCommand();

    var args = new String[] {"dump", "12345", "23456"};
    // var output = cmd.dumpTx(zs, args);
    // System.out.println(output);
    // assertTrue(output.contains("0000000000012345"));
    // assertTrue(output.contains("0000000000023456"));
    //
    // verify(zs);
  }

  static class TestHelper extends AdminUtil<FateCommand> {

    public TestHelper(boolean exitOnError) {
      super(exitOnError);
    }

    @Override
    public boolean checkGlobalLock(ZooReaderWriter zk, ServiceLockPath zLockManagerPath) {
      return true;
    }
  }
}
