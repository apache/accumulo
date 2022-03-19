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

import static org.apache.accumulo.fate.zookeeper.ServiceLock.ServiceLockPath;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ReadOnlyRepo;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.ServiceLock.ServiceLockPath;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.ShellConfigTest.TestOutputStream;
import org.apache.commons.cli.CommandLine;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FateCommandTest {
  private static FateCommand cmd;
  private static Shell shellState;
  private static LineReader reader;
  private static Terminal terminal;
  private static PrintWriter pw;
  private static AccumuloClient client;
  private static InstanceOperations intOps;

  public static class TestFateCommand extends FateCommand {

    private boolean dumpCalled = false;
    private boolean deleteCalled = false;
    private boolean failCalled = false;
    private boolean cancelCalled = false;
    private boolean printCalled = false;

    @Override
    public String getName() {
      return "fate";
    }

    @Override
    protected void dumpTx(Shell shellState, String[] args) {
      dumpCalled = true;
    }

    @Override
    protected void deleteTx(Shell shellState, String[] args) {
      deleteCalled = true;
    }

    @Override
    protected void cancelSubmittedTxs(Shell shellState, String[] args) {
      cancelCalled = true;
    }

    @Override
    public void failTx(Shell shellState, String[] args) {
      failCalled = true;
    }

    @Override
    protected void printTx(Shell shellState, String[] args, CommandLine cl, boolean printStatus) {
      printCalled = true;
    }

    public void reset() {
      dumpCalled = false;
      deleteCalled = false;
      failCalled = false;
      cancelCalled = false;
      printCalled = false;
    }

  }

  private static ZooReaderWriter zk;
  private static ServiceLockPath managerLockPath;

  @BeforeAll
  public static void setup() {
    zk = createMock(ZooReaderWriter.class);
    managerLockPath = createMock(ServiceLockPath.class);
  }

  @Test
  public void testFailTx() throws Exception {
    client = createMock(AccumuloClient.class);
    CommandLine cli = createMock(CommandLine.class);
    shellState = createMock(Shell.class);
    reader = createMock(LineReader.class);
    pw = createMock(PrintWriter.class);
    intOps = createMock(InstanceOperations.class);
    TestFateCommand cmd = new TestFateCommand();
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
    expectLastCall().once();
    expect(shellState.getAccumuloClient()).andReturn(client);
    expect(client.instanceOperations()).andReturn(intOps);
    intOps.fateFail(txids);
    expectLastCall().once();

//    replay(client, cli, shellState, reader, intOps);
//    //cmd.execute("fate --fail 1234", cli, shellState);
//    verify(client, cli, shellState, reader, intOps);
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

  @Test
  public void testCommandLineOptions() throws Exception {
    PrintStream out = System.out;
    TestOutputStream output = new TestOutputStream();
    System.setOut(new PrintStream(output));
    File config = Files.createTempFile(null, null).toFile();
    Terminal terminal = new DumbTerminal(new FileInputStream(FileDescriptor.in), output);
    terminal.setSize(new Size(80, 24));
    LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();
    Shell shell = new Shell(reader);
    shell.setLogErrorsToConsole();
    try {
      assertTrue(shell.config("--config-file", config.toString(), "-zh", "127.0.0.1:2181", "-zi",
          "test", "-u", "test", "-p", "password"));
      TestFateCommand cmd = new TestFateCommand();
      shell.commandFactory.clear();
      shell.commandFactory.put("fate", cmd);
      shell.execCommand("fate -?", true, false);
      Shell.log.info("{}", output.get());
      shell.execCommand("fate --help", true, false);
      shell.execCommand("fate cancel", true, false);
      assertFalse(cmd.cancelCalled);
      cmd.reset();
      shell.execCommand("fate -cancel", true, false);
      assertFalse(cmd.cancelCalled);
      cmd.reset();
      shell.execCommand("fate -cancel 12345", true, false);
      assertTrue(cmd.cancelCalled);
      cmd.reset();
      shell.execCommand("fate --cancel-submitted 12345 67890", true, false);
      assertTrue(cmd.cancelCalled);
      cmd.reset();
      shell.execCommand("fate delete", true, false);
      assertFalse(cmd.deleteCalled);
      cmd.reset();
      shell.execCommand("fate -delete", true, false);
      assertFalse(cmd.deleteCalled);
      cmd.reset();
      shell.execCommand("fate -delete 12345", true, false);
      assertTrue(cmd.deleteCalled);
      cmd.reset();
      shell.execCommand("fate --delete 12345 67890", true, false);
      assertTrue(cmd.deleteCalled);
      cmd.reset();
      shell.execCommand("fate dump", true, false);
      assertFalse(cmd.dumpCalled);
      cmd.reset();
      shell.execCommand("fate -dump", true, false);
      assertTrue(cmd.dumpCalled);
      cmd.reset();
      shell.execCommand("fate -dump 12345", true, false);
      assertTrue(cmd.dumpCalled);
      cmd.reset();
      shell.execCommand("fate --dump 12345 67890", true, false);
      assertTrue(cmd.dumpCalled);
      cmd.reset();
      shell.execCommand("fate fail", true, false);
      assertFalse(cmd.failCalled);
      cmd.reset();
      shell.execCommand("fate -fail", true, false);
      assertFalse(cmd.failCalled);
      cmd.reset();
      shell.execCommand("fate -fail 12345", true, false);
      assertTrue(cmd.failCalled);
      cmd.reset();
      shell.execCommand("fate --fail 12345 67890", true, false);
      assertTrue(cmd.failCalled);
      cmd.reset();
      shell.execCommand("fate print", true, false);
      assertFalse(cmd.printCalled);
      cmd.reset();
      shell.execCommand("fate -print", true, false);
      assertTrue(cmd.printCalled);
      cmd.reset();
      shell.execCommand("fate --print", true, false);
      assertTrue(cmd.printCalled);
      cmd.reset();
      shell.execCommand("fate --print 12345 67890", true, false);
      assertTrue(cmd.printCalled);
      cmd.reset();
      shell.execCommand("fate list", true, false);
      assertFalse(cmd.printCalled);
      cmd.reset();
      shell.execCommand("fate -list", true, false);
      assertTrue(cmd.printCalled);
      cmd.reset();
      shell.execCommand("fate --list", true, false);
      assertTrue(cmd.printCalled);
      cmd.reset();
      shell.execCommand("fate --list 12345 67890", true, false);
      assertTrue(cmd.printCalled);
      cmd.reset();
    } finally {
      shell.shutdown();
      output.clear();
      System.setOut(out);
      if (config.exists()) {
        assertTrue(config.delete());
      }
    }
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
