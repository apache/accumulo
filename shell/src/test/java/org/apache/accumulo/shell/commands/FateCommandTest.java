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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.FateTransaction;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.FateTransactionImpl;
import org.apache.accumulo.core.data.FateTxId;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ReadOnlyTStore;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.ServiceLock.ServiceLockPath;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
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
    private boolean cancelCalled = false;
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
    protected String dumpTx(Shell shellState, String[] args) {
      dumpCalled = true;
      return "";
    }

    @Override
    protected boolean deleteTx(Shell shell, String[] args) {
      deleteCalled = true;
      return true;
    }

    @Override
    protected boolean cancelSubmittedTxs(Shell shellState, String[] args)
        throws AccumuloException, AccumuloSecurityException {
      cancelCalled = true;
      return true;
    }

    @Override
    public boolean failTx(Shell shellState, List<String> txids) {
      failCalled = true;
      return true;
    }

    @Override
    protected void printTx(Shell shellState, String[] args, CommandLine cl) {
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

  private FateTransaction createFateTx(FateTxId txId, FateTransaction.Status status) {
    return new FateTransactionImpl(txId.canonical(), status.name(), "debug", List.of(), List.of(),
        "String top", System.currentTimeMillis(), "String stackInfo");
  }

  public static class TestFateTransaction implements FateTransaction {
    FateTxId id;
    FateTransaction.Status status;

    public TestFateTransaction(String txString) {
      this.id = FateTxId.parse(txString);
    }

    public TestFateTransaction(FateTxId tx1, Status status) {
      this.id = tx1;
      this.status = status;
    }

    @Override
    public FateTxId getId() {
      return id;
    }

    @Override
    public Status getStatus() {
      return status;
    }

    @Override
    public String getDebug() {
      return null;
    }

    @Override
    public List<String> getHeldLocks() {
      return null;
    }

    @Override
    public List<String> getWaitingLocks() {
      return null;
    }

    @Override
    public String getTop() {
      return null;
    }

    @Override
    public String getTimeCreatedFormatted() {
      return null;
    }

    @Override
    public long getTimeCreated() {
      return 0;
    }

    @Override
    public String getStackInfo() {
      return null;
    }

    @Override
    public void fail() {

    }

    @Override
    public void delete() throws AccumuloException {

    }
  }

  private static ZooReaderWriter zk;
  private static ServiceLockPath managerLockPath;

  @BeforeAll
  public static void setup() {
    zk = createMock(ZooReaderWriter.class);
    managerLockPath = createMock(ServiceLockPath.class);
  }

  private TestFateTransaction createFateTx(String userTxId) {
    return new TestFateTransaction("12345");
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

    // TestHelper helper = new TestHelper(true);

    replay(zs);

    FateCommand cmd = new FateCommand();
    // require number for Tx
    // assertFalse(cmd.failTx(helper, zs, zk, managerLockPath, new String[] {"fail", "tx1"}));
    // TODO get shell
    assertFalse(cmd.failTx(createShell(null), List.of("fail", "tx1")));
    // fail the long configured above
    // assertTrue(cmd.failTx(helper, zs, zk, managerLockPath, new String[] {"fail", "12345"}));

    verify(zs);
  }

  @Test
  public void testDump() throws AccumuloException {
    // ZooStore<FateCommand> zs = createMock(ZooStore.class);
    // ReadOnlyRepo<FateCommand> ser = createMock(ReadOnlyRepo.class);
    // long tid1 = Long.parseLong("12345", 16);
    // long tid2 = Long.parseLong("23456", 16);
    // expect(zs.getStack(tid1)).andReturn(List.of(ser)).once();
    // expect(zs.getStack(tid2)).andReturn(List.of(ser)).once();

    // replay(zs);

    FateCommand cmd = new FateCommand();
    Set<FateTransaction> running = new HashSet<>();
    running.add(createFateTx("12345"));
    running.add(createFateTx("23456"));

    var args = new String[] {"dump", "12345", "23456"};
    var output = cmd.dumpTx(running, args);
    System.out.println(output);
    assertTrue(output.contains("0000000000012345"));
    assertTrue(output.contains("0000000000023456"));

    // verify(zs);
  }

  @Test
  public void testGetTx() {
    FateCommand cmd = new FateCommand();
    List<String> userFilters = new ArrayList<>();
    userFilters.add(FateTransaction.Status.NEW.name());
    userFilters.add(FateTransaction.Status.IN_PROGRESS.name());
    Set<String> userProvidedIds = new HashSet<>(List.of("12345", "12346"));
    Set<FateTransaction> running = new HashSet<>();

    // test empty filters, nothing running
    Set<FateTransactionImpl> results = cmd.getTransactions(running, Set.of(), List.of());
    assertEquals(0, results.size());

    var tx1Id = FateTxId.parse("0000000000012345");
    var tx2Id = FateTxId.parse("0000000000012346");
    var tx1 = createFateTx(tx1Id, FateTransaction.Status.NEW);
    var tx2 = createFateTx(tx2Id, FateTransaction.Status.IN_PROGRESS);

    running.add(tx1);
    running.add(tx2);

    // test return everything
    results = cmd.getTransactions(running, Set.of(), List.of());
    assertEquals(2, results.size());
    assertTrue(results.contains((FateTransactionImpl) tx1));
    assertTrue(results.contains((FateTransactionImpl) tx2));

    // test return only 1
    results = cmd.getTransactions(running, new HashSet<>(List.of("12345")), List.of());
    assertEquals(1, results.size());
    assertTrue(results.contains((FateTransactionImpl) tx1));

    // test return with user options
    results = cmd.getTransactions(running, userProvidedIds, userFilters);
    assertEquals(2, results.size());
    assertTrue(results.contains((FateTransactionImpl) tx1));
    assertTrue(results.contains((FateTransactionImpl) tx2));
  }

  @Test
  public void testPrintAndList() throws IOException, InterruptedException, KeeperException {
    PrintStream out = System.out;
    File config = Files.createTempFile(null, null).toFile();
    TestOutputStream output = new TestOutputStream();
    Shell shell = createShell(output);
    // Shell shell = createMock(Shell.class);
    AccumuloClient client = createMock(AccumuloClient.class);
    InstanceOperations ops = createMock(InstanceOperations.class);
    Set<FateTransaction> fates = new HashSet<>();

    // ServiceLockPath tableLocksPath = ServiceLock.path("/accumulo" + ZTABLE_LOCKS);
    // ZooStore<FateCommand> zs = createMock(ZooStore.class);
    // expect(zk.getChildren(tableLocksPath.toString())).andReturn(List.of("5")).anyTimes();
    // expect(zk.getChildren("/accumulo/table_locks/5")).andReturn(List.of()).anyTimes();
    // expect(zs.list()).andReturn(List.of()).anyTimes();
    expect(shell.getAccumuloClient()).andReturn(client).once();
    expect(client.instanceOperations()).andReturn(ops).once();
    expect(ops.getFateTransactions()).andReturn(fates).once();

    // replay(zs, zk);
    replay(shell, client, ops);

    // TestHelper helper = new TestHelper(true);
    FateCommand cmd = new FateCommand();
    var options = cmd.getOptions();
    CommandLine cli = new CommandLine.Builder().addOption(options.getOption("list"))
        .addOption(options.getOption("print")).addOption(options.getOption("np")).build();

    try {
      cmd.printTx(shell, cli.getOptionValues("list"), cli);
      cmd.printTx(shell, cli.getOptionValues("print"), cli);
      cmd.printTx(shell, new String[] {""}, cli);
      cmd.printTx(shell, new String[] {}, cli);
      cmd.printTx(shell, null, cli);
      cmd.printTx(shell, new String[] {"list"}, cli);
      cmd.printTx(shell, new String[] {"list", "1234"}, cli);
      cmd.printTx(shell, new String[] {"print"}, cli);
      cmd.printTx(shell, new String[] {"print", "1234"}, cli);
    } catch (AccumuloException e) {
      throw new RuntimeException(e);
    } finally {
      output.clear();
      System.setOut(out);
      if (config.exists()) {
        assertTrue(config.delete());
      }
    }

    // verify(zs, zk);
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
