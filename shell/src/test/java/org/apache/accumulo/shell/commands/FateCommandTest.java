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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.clientImpl.TransactionStatusImpl;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.ShellConfigTest.TestOutputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.zookeeper.KeeperException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
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

  @Test
  public void testFailTx() throws Exception {
    AccumuloClient client = createMock(AccumuloClient.class);
    Shell shellState = createMock(Shell.class);
    InstanceOperations intOps = createMock(InstanceOperations.class);
    var args = new String[] {"--fail", "12345"};

    FateCommand cmd = new FateCommand();
    Options opts = cmd.getOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cli = parser.parse(opts, args);

    expect(shellState.getAccumuloClient()).andReturn(client);
    expect(client.instanceOperations()).andReturn(intOps);
    intOps.fateFail(new HashSet<>(Arrays.asList(cli.getOptionValues("fail"))));
    expectLastCall().once();

    replay(client, shellState, intOps);
    cmd.execute("fate --fail 1234", cli, shellState);
    verify(client, shellState, intOps);
  }

  @Test
  public void testDump() throws AccumuloException, AccumuloSecurityException, ParseException,
      IOException, InterruptedException, KeeperException {
    AccumuloClient client = createMock(AccumuloClient.class);
    Shell shellState = createMock(Shell.class);
    InstanceOperations intOps = createMock(InstanceOperations.class);
    PrintWriter pw = createMock(PrintWriter.class);
    List<TransactionStatusImpl> txstatus = new ArrayList<>();
    var args = new String[] {"--dump", "12345"};

    FateCommand cmd = new FateCommand();
    Options opts = cmd.getOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cli = parser.parse(opts, args);

    expect(shellState.getAccumuloClient()).andReturn(client).anyTimes();
    expect(shellState.getWriter()).andReturn(pw);
    expect(client.instanceOperations()).andReturn(intOps).anyTimes();

    txstatus.add(new TransactionStatusImpl(Long.parseLong("1234"), null, null,
        Collections.emptyList(), Collections.emptyList(), null, System.currentTimeMillis(), null));
    expect(intOps.fateStatus(new HashSet<>(Arrays.asList(cli.getOptionValues("dump"))), null))
        .andReturn(txstatus);
    pw.println(txstatus.get(0).getStackInfo());
    expectLastCall().once();

    replay(shellState, pw, client, intOps);
    cmd.execute("fate --dump 1234", cli, shellState);
    verify(shellState, pw, client, intOps);
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
}
