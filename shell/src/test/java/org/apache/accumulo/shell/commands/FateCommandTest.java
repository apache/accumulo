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

import static org.apache.accumulo.core.Constants.ZTABLE_LOCKS;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.List;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.ReadOnlyRepo;
import org.apache.accumulo.core.fate.ReadOnlyTStore;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.ServiceLockPath;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.ShellConfigTest.TestOutputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.zookeeper.KeeperException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FateCommandTest {

  public static class TestFateCommand extends FateCommand {

    private boolean dumpCalled = false;
    private boolean deleteCalled = false;
    private boolean failCalled = false;
    private boolean printCalled = false;

    @Override
    public String getName() {
      return "fate";
    }

    @Override
    protected String getZKRoot(ClientContext context) {
      return "";
    }

    @Override
    synchronized ZooReaderWriter getZooReaderWriter(ClientContext context, String secret) {
      return null;
    }

    @Override
    protected ZooStore<FateCommand> getZooStore(String fateZkPath, ZooReaderWriter zrw)
        throws KeeperException, InterruptedException {
      return null;
    }

    @Override
    String dumpTx(ZooStore<FateCommand> zs, String[] args) {
      dumpCalled = true;
      return "";
    }

    @Override
    protected boolean deleteTx(AdminUtil<FateCommand> admin, ZooStore<FateCommand> zs,
        ZooReaderWriter zk, ServiceLockPath zLockManagerPath, String[] args)
        throws InterruptedException, KeeperException {
      deleteCalled = true;
      return true;
    }

    @Override
    public boolean failTx(AdminUtil<FateCommand> admin, ZooStore<FateCommand> zs,
        ZooReaderWriter zk, ServiceLockPath managerLockPath, String[] args) {
      failCalled = true;
      return true;
    }

    @Override
    protected void printTx(Shell shellState, AdminUtil<FateCommand> admin, ZooStore<FateCommand> zs,
        ZooReaderWriter zk, ServiceLockPath tableLocksPath, String[] args, CommandLine cl)
        throws InterruptedException, KeeperException, IOException {
      printCalled = true;
    }

    public void reset() {
      dumpCalled = false;
      deleteCalled = false;
      failCalled = false;
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
    ZooStore<FateCommand> zs = createMock(ZooStore.class);
    String tidStr = "12345";
    long tid = Long.parseLong(tidStr, 16);
    expect(zs.getStatus(tid)).andReturn(ReadOnlyTStore.TStatus.NEW).anyTimes();
    zs.reserve(tid);
    expectLastCall().once();
    zs.setStatus(tid, ReadOnlyTStore.TStatus.FAILED_IN_PROGRESS);
    expectLastCall().once();
    zs.unreserve(tid, 0);
    expectLastCall().once();

    TestHelper helper = new TestHelper(true);

    replay(zs);

    FateCommand cmd = new FateCommand();
    // require number for Tx
    assertFalse(cmd.failTx(helper, zs, zk, managerLockPath, new String[] {"fail", "tx1"}));
    // fail the long configured above
    assertTrue(cmd.failTx(helper, zs, zk, managerLockPath, new String[] {"fail", "12345"}));

    verify(zs);
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

    FateCommand cmd = new FateCommand();

    var args = new String[] {"dump", "12345", "23456"};
    var output = cmd.dumpTx(zs, args);
    System.out.println(output);
    assertTrue(output.contains("0000000000012345"));
    assertTrue(output.contains("0000000000023456"));

    verify(zs);
  }

  @Test
  public void testPrintAndList() throws IOException, InterruptedException, KeeperException {
    reset(zk);
    PrintStream out = System.out;
    File config = Files.createTempFile(null, null).toFile();
    TestOutputStream output = new TestOutputStream();
    Shell shell = createShell(output);

    ServiceLockPath tableLocksPath = ServiceLock.path("/accumulo" + ZTABLE_LOCKS);
    ZooStore<FateCommand> zs = createMock(ZooStore.class);
    expect(zk.getChildren(tableLocksPath.toString())).andReturn(List.of("5")).anyTimes();
    expect(zk.getChildren("/accumulo/table_locks/5")).andReturn(List.of()).anyTimes();
    expect(zs.list()).andReturn(List.of()).anyTimes();

    replay(zs, zk);

    TestHelper helper = new TestHelper(true);
    FateCommand cmd = new FateCommand();
    var options = cmd.getOptions();
    CommandLine cli = new CommandLine.Builder().addOption(options.getOption("list"))
        .addOption(options.getOption("print")).addOption(options.getOption("np")).build();

    try {
      cmd.printTx(shell, helper, zs, zk, tableLocksPath, cli.getOptionValues("list"), cli);
      cmd.printTx(shell, helper, zs, zk, tableLocksPath, cli.getOptionValues("list FATE[1]"), cli);
      cmd.printTx(shell, helper, zs, zk, tableLocksPath, cli.getOptionValues("list 1234"), cli);
      cmd.printTx(shell, helper, zs, zk, tableLocksPath, cli.getOptionValues("list 1234 2345"),
          cli);
      cmd.printTx(shell, helper, zs, zk, tableLocksPath, cli.getOptionValues("print"), cli);
      cmd.printTx(shell, helper, zs, zk, tableLocksPath, cli.getOptionValues("print FATE[1]"), cli);
      cmd.printTx(shell, helper, zs, zk, tableLocksPath, cli.getOptionValues("print 1234"), cli);
      cmd.printTx(shell, helper, zs, zk, tableLocksPath, cli.getOptionValues("print 1234 2345"),
          cli);
      cmd.printTx(shell, helper, zs, zk, tableLocksPath, new String[] {""}, cli);
      cmd.printTx(shell, helper, zs, zk, tableLocksPath, new String[] {}, cli);
      cmd.printTx(shell, helper, zs, zk, tableLocksPath, null, cli);
    } finally {
      output.clear();
      System.setOut(out);
      if (config.exists()) {
        assertTrue(config.delete());
      }
    }

    verify(zs, zk);
  }

  @Test
  public void testCommandLineOptions() throws Exception {
    PrintStream out = System.out;
    File config = Files.createTempFile(null, null).toFile();
    TestOutputStream output = new TestOutputStream();

    Shell shell = createShell(output);
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

  private Shell createShell(TestOutputStream output) throws IOException {
    System.setOut(new PrintStream(output));
    Terminal terminal = new DumbTerminal(new FileInputStream(FileDescriptor.in), output);
    terminal.setSize(new Size(80, 24));
    LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();
    Shell shell = new Shell(reader);
    shell.setLogErrorsToConsole();
    return shell;
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
