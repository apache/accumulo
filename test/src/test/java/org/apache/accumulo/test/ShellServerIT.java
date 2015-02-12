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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jline.console.ConsoleReader;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.harness.SharedMiniClusterIT;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.tracer.TraceServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.DistCp;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class ShellServerIT extends SharedMiniClusterIT {
  public static class TestOutputStream extends OutputStream {
    StringBuilder sb = new StringBuilder();

    @Override
    public void write(int b) throws IOException {
      sb.append((char) (0xff & b));
    }

    public String get() {
      return sb.toString();
    }

    public void clear() {
      sb.setLength(0);
    }
  }

  private static final Logger log = Logger.getLogger(ShellServerIT.class);

  public static class StringInputStream extends InputStream {
    private String source = "";
    private int offset = 0;

    @Override
    public int read() throws IOException {
      if (offset == source.length())
        return '\n';
      else
        return source.charAt(offset++);
    }

    public void set(String other) {
      source = other;
      offset = 0;
    }
  }

  private static abstract class ErrorMessageCallback {
    public abstract String getErrorMessage();
  }

  private static class NoOpErrorMessageCallback extends ErrorMessageCallback {
    private static final String empty = "";

    @Override
    public String getErrorMessage() {
      return empty;
    }
  }

  public static class TestShell {
    public TestOutputStream output;
    public StringInputStream input;
    public Shell shell;

    TestShell(String rootPass, String instanceName, String zookeepers, String configFile) throws IOException {
      // start the shell
      output = new TestOutputStream();
      input = new StringInputStream();
      PrintWriter pw = new PrintWriter(new OutputStreamWriter(output));
      shell = new Shell(new ConsoleReader(input, output), pw);
      shell.setLogErrorsToConsole();
      shell.config("-u", "root", "-p", rootPass, "-z", instanceName, zookeepers, "--config-file", configFile);
      exec("quit", true);
      shell.start();
      shell.setExit(false);
    }

    String exec(String cmd) throws IOException {
      output.clear();
      shell.execCommand(cmd, true, true);
      return output.get();
    }

    String exec(String cmd, boolean expectGoodExit) throws IOException {
      return exec(cmd, expectGoodExit, noop);
    }

    String exec(String cmd, boolean expectGoodExit, ErrorMessageCallback callback) throws IOException {
      String result = exec(cmd);
      if (expectGoodExit)
        assertGoodExit("", true, callback);
      else
        assertBadExit("", true, callback);
      return result;
    }

    String exec(String cmd, boolean expectGoodExit, String expectString) throws IOException {
      return exec(cmd, expectGoodExit, expectString, noop);
    }

    String exec(String cmd, boolean expectGoodExit, String expectString, ErrorMessageCallback callback) throws IOException {
      return exec(cmd, expectGoodExit, expectString, true, callback);
    }

    String exec(String cmd, boolean expectGoodExit, String expectString, boolean stringPresent) throws IOException {
      return exec(cmd, expectGoodExit, expectString, stringPresent, noop);
    }

    String exec(String cmd, boolean expectGoodExit, String expectString, boolean stringPresent, ErrorMessageCallback callback) throws IOException {
      String result = exec(cmd);
      if (expectGoodExit)
        assertGoodExit(expectString, stringPresent, callback);
      else
        assertBadExit(expectString, stringPresent, callback);
      return result;
    }

    void assertGoodExit(String s, boolean stringPresent) {
      assertGoodExit(s, stringPresent, noop);
    }

    void assertGoodExit(String s, boolean stringPresent, ErrorMessageCallback callback) {
      Shell.log.info(output.get());
      if (0 != shell.getExitCode()) {
        String errorMsg = callback.getErrorMessage();
        assertEquals(errorMsg, 0, shell.getExitCode());
      }

      if (s.length() > 0)
        assertEquals(s + " present in " + output.get() + " was not " + stringPresent, stringPresent, output.get().contains(s));
    }

    void assertBadExit(String s, boolean stringPresent, ErrorMessageCallback callback) {
      Shell.log.debug(output.get());
      if (0 == shell.getExitCode()) {
        String errorMsg = callback.getErrorMessage();
        assertTrue(errorMsg, shell.getExitCode() > 0);
      }

      if (s.length() > 0)
        assertEquals(s + " present in " + output.get() + " was not " + stringPresent, stringPresent, output.get().contains(s));
      shell.resetExitCode();
    }
  }

  private static final NoOpErrorMessageCallback noop = new NoOpErrorMessageCallback();

  private TestShell ts;

  private static Process traceProcess;
  private static String rootPath;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    rootPath = getMiniClusterDir().getAbsolutePath();

    // history file is updated in $HOME
    System.setProperty("HOME", rootPath);
    System.setProperty("hadoop.tmp.dir", System.getProperty("user.dir") + "/target/hadoop-tmp");

    traceProcess = getCluster().exec(TraceServer.class);

    Connector conn = getCluster().getConnector("root", getToken());
    TableOperations tops = conn.tableOperations();

    // give the tracer some time to start
    while (!tops.exists("trace")) {
      UtilWaitThread.sleep(1000);
    }
  }

  @Before
  public void setupShell() throws Exception {
    ts = new TestShell(getRootPassword(), getCluster().getConfig().getInstanceName(), getCluster().getConfig().getZooKeepers(), getCluster().getConfig()
        .getClientConfFile().getAbsolutePath());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (null != traceProcess) {
      traceProcess.destroy();
    }
  }

  @After
  public void deleteTables() throws Exception {
    Connector c = getConnector();
    for (String table : c.tableOperations().list()) {
      if (!table.equals(MetadataTable.NAME) && !table.equals(RootTable.NAME) && !table.equals("trace"))
        try {
          c.tableOperations().delete(table);
        } catch (TableNotFoundException e) {
          // don't care
        }
    }
  }

  @After
  public void tearDownShell() {
    ts.shell.shutdown();
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void exporttableImporttable() throws Exception {
    final String table = name.getMethodName(), table2 = table + "2";

    // exporttable / importtable
    ts.exec("createtable " + table + " -evc", true);
    make10();
    ts.exec("addsplits row5", true);
    ts.exec("config -t " + table + " -s table.split.threshold=345M", true);
    ts.exec("offline " + table, true);
    String export = "file://" + new File(rootPath, "ShellServerIT.export").toString();
    ts.exec("exporttable -t " + table + " " + export, true);
    DistCp cp = newDistCp();
    String import_ = "file://" + new File(rootPath, "ShellServerIT.import").toString();
    cp.run(new String[] {"-f", export + "/distcp.txt", import_});
    ts.exec("importtable " + table2 + " " + import_, true);
    ts.exec("config -t " + table2 + " -np", true, "345M", true);
    ts.exec("getsplits -t " + table2, true, "row5", true);
    ts.exec("constraint --list -t " + table2, true, "VisibilityConstraint=2", true);
    ts.exec("onlinetable " + table, true);
    ts.exec("deletetable -f " + table, true);
    ts.exec("deletetable -f " + table2, true);
  }

  private DistCp newDistCp() {
    try {
      @SuppressWarnings("unchecked")
      Constructor<DistCp>[] constructors = (Constructor<DistCp>[]) DistCp.class.getConstructors();
      for (Constructor<DistCp> constructor : constructors) {
        Class<?>[] parameterTypes = constructor.getParameterTypes();
        if (parameterTypes.length > 0 && parameterTypes[0].equals(Configuration.class)) {
          if (parameterTypes.length == 1) {
            return constructor.newInstance(new Configuration());
          } else if (parameterTypes.length == 2) {
            return constructor.newInstance(new Configuration(), null);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    throw new RuntimeException("Unexpected constructors for DistCp");
  }

  @Test
  public void setscaniterDeletescaniter() throws Exception {
    final String table = name.getMethodName();

    // setscaniter, deletescaniter
    ts.exec("createtable " + table);
    ts.exec("insert a cf cq 1");
    ts.exec("insert a cf cq 1");
    ts.exec("insert a cf cq 1");
    ts.input.set("true\n\n\nSTRING");
    ts.exec("setscaniter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -n name", true);
    ts.exec("scan", true, "3", true);
    ts.exec("deletescaniter -n name", true);
    ts.exec("scan", true, "1", true);
    ts.exec("deletetable -f " + table);

  }

  @Test
  public void execfile() throws Exception {
    // execfile
    File file = File.createTempFile("ShellServerIT.execfile", ".conf", new File(rootPath));
    PrintWriter writer = new PrintWriter(file.getAbsolutePath());
    writer.println("about");
    writer.close();
    ts.exec("execfile " + file.getAbsolutePath(), true, Constants.VERSION, true);

  }

  @Test
  public void egrep() throws Exception {
    final String table = name.getMethodName();

    // egrep
    ts.exec("createtable " + table);
    make10();
    String lines = ts.exec("egrep row[123]", true);
    assertTrue(lines.split("\n").length - 1 == 3);
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void du() throws Exception {
    final String table = name.getMethodName();

    // create and delete a table so we get out of a table context in the shell
    ts.exec("notable", true);

    // Calling du not in a table context shouldn't throw an error
    ts.output.clear();
    ts.exec("du", true, "", true);

    ts.output.clear();
    ts.exec("createtable " + table);
    make10();
    ts.exec("flush -t " + table + " -w");
    ts.exec("du " + table, true, " [" + table + "]", true);
    ts.output.clear();
    ts.shell.execCommand("du -h", false, false);
    String o = ts.output.get();
    // for some reason, there's a bit of fluctuation
    assertTrue("Output did not match regex: '" + o + "'", o.matches(".*[1-9][0-9][0-9]\\s\\[" + table + "\\]\\n"));
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void debug() throws Exception {
    ts.exec("debug", true, "off", true);
    ts.exec("debug on", true);
    ts.exec("debug", true, "on", true);
    ts.exec("debug off", true);
    ts.exec("debug", true, "off", true);
    ts.exec("debug debug", false);
    ts.exec("debug debug debug", false);
  }

  @Test
  public void user() throws Exception {
    final String table = name.getMethodName();

    // createuser, deleteuser, user, users, droptable, grant, revoke
    ts.input.set("secret\nsecret\n");
    ts.exec("createuser xyzzy", true);
    ts.exec("users", true, "xyzzy", true);
    String perms = ts.exec("userpermissions -u xyzzy", true);
    assertTrue(perms.contains("Table permissions (" + MetadataTable.NAME + "): Table.READ"));
    ts.exec("grant -u xyzzy -s System.CREATE_TABLE", true);
    perms = ts.exec("userpermissions -u xyzzy", true);
    assertTrue(perms.contains(""));
    ts.exec("grant -u root -t " + MetadataTable.NAME + " Table.WRITE", true);
    ts.exec("grant -u root -t " + MetadataTable.NAME + " Table.GOOFY", false);
    ts.exec("grant -u root -s foo", false);
    ts.exec("grant -u xyzzy -t " + MetadataTable.NAME + " foo", false);
    ts.input.set("secret\nsecret\n");
    ts.exec("user xyzzy", true);
    ts.exec("createtable " + table, true, "xyzzy@", true);
    ts.exec("insert row1 cf cq 1", true);
    ts.exec("scan", true, "row1", true);
    ts.exec("droptable -f " + table, true);
    ts.exec("deleteuser xyzzy", false, "delete yourself", true);
    ts.input.set(getRootPassword() + "\n" + getRootPassword() + "\n");
    ts.exec("user root", true);
    ts.exec("revoke -u xyzzy -s System.CREATE_TABLE", true);
    ts.exec("revoke -u xyzzy -s System.GOOFY", false);
    ts.exec("revoke -u xyzzy -s foo", false);
    ts.exec("revoke -u xyzzy -t " + MetadataTable.NAME + " Table.WRITE", true);
    ts.exec("revoke -u xyzzy -t " + MetadataTable.NAME + " Table.GOOFY", false);
    ts.exec("revoke -u xyzzy -t " + MetadataTable.NAME + " foo", false);
    ts.exec("deleteuser xyzzy", true);
    ts.exec("users", true, "xyzzy", false);
  }

  @Test
  public void iter() throws Exception {
    final String table = name.getMethodName();

    // setshelliter, listshelliter, deleteshelliter
    ts.exec("createtable " + table);
    ts.exec("insert a cf cq 1");
    ts.exec("insert a cf cq 1");
    ts.exec("insert a cf cq 1");
    ts.input.set("true\n\n\nSTRING\n");
    ts.exec("setshelliter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -pn sum -n name", true);
    ts.exec("setshelliter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 11 -pn sum -n name", false);
    ts.exec("setshelliter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -pn sum -n other", false);
    ts.input.set("true\n\n\nSTRING\n");
    ts.exec("setshelliter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 11 -pn sum -n xyzzy", true);
    ts.exec("scan -pn sum", true, "3", true);
    ts.exec("listshelliter", true, "Iterator name", true);
    ts.exec("listshelliter", true, "Iterator xyzzy", true);
    ts.exec("listshelliter", true, "Profile : sum", true);
    ts.exec("deleteshelliter -pn sum -n name", true);
    ts.exec("listshelliter", true, "Iterator name", false);
    ts.exec("listshelliter", true, "Iterator xyzzy", true);
    ts.exec("deleteshelliter -pn sum -a", true);
    ts.exec("listshelliter", true, "Iterator xyzzy", false);
    ts.exec("listshelliter", true, "Profile : sum", false);
    ts.exec("deletetable -f " + table);
    // list iter
    ts.exec("createtable " + table);
    ts.exec("insert a cf cq 1");
    ts.exec("insert a cf cq 1");
    ts.exec("insert a cf cq 1");
    ts.input.set("true\n\n\nSTRING\n");
    ts.exec("setiter -scan -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -n name", true);
    ts.exec("setiter -scan -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 11 -n name", false);
    ts.exec("setiter -scan -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -n other", false);
    ts.input.set("true\n\n\nSTRING\n");
    ts.exec("setiter -scan -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 11 -n xyzzy", true);
    ts.exec("scan", true, "3", true);
    ts.exec("listiter -scan", true, "Iterator name", true);
    ts.exec("listiter -scan", true, "Iterator xyzzy", true);
    ts.exec("listiter -minc", true, "Iterator name", false);
    ts.exec("listiter -minc", true, "Iterator xyzzy", false);
    ts.exec("deleteiter -scan -n name", true);
    ts.exec("listiter -scan", true, "Iterator name", false);
    ts.exec("listiter -scan", true, "Iterator xyzzy", true);
    ts.exec("deletetable -f " + table);

  }

  @Test
  public void setIterOptionPrompt() throws Exception {
    Connector conn = getConnector();
    String tableName = name.getMethodName();

    ts.exec("createtable " + tableName);
    ts.input.set("\n\n");
    // Setting a non-optiondescriber with no name should fail
    ts.exec("setiter -scan -class org.apache.accumulo.core.iterators.ColumnFamilyCounter -p 30", false);

    // Name as option will work
    ts.exec("setiter -scan -class org.apache.accumulo.core.iterators.ColumnFamilyCounter -p 30 -name cfcounter", true);

    String expectedKey = "table.iterator.scan.cfcounter";
    String expectedValue = "30,org.apache.accumulo.core.iterators.ColumnFamilyCounter";
    TableOperations tops = conn.tableOperations();
    checkTableForProperty(tops, tableName, expectedKey, expectedValue);

    ts.exec("deletetable " + tableName, true);
    tableName = tableName + "1";

    ts.exec("createtable " + tableName, true);

    ts.input.set("customcfcounter\n\n");

    // Name on the CLI should override OptionDescriber (or user input name, in this case)
    ts.exec("setiter -scan -class org.apache.accumulo.core.iterators.ColumnFamilyCounter -p 30", true);
    expectedKey = "table.iterator.scan.customcfcounter";
    expectedValue = "30,org.apache.accumulo.core.iterators.ColumnFamilyCounter";
    checkTableForProperty(tops, tableName, expectedKey, expectedValue);

    ts.exec("deletetable " + tableName, true);
    tableName = tableName + "1";

    ts.exec("createtable " + tableName, true);

    ts.input.set("customcfcounter\nname1 value1\nname2 value2\n\n");

    // Name on the CLI should override OptionDescriber (or user input name, in this case)
    ts.exec("setiter -scan -class org.apache.accumulo.core.iterators.ColumnFamilyCounter -p 30", true);
    expectedKey = "table.iterator.scan.customcfcounter";
    expectedValue = "30,org.apache.accumulo.core.iterators.ColumnFamilyCounter";
    checkTableForProperty(tops, tableName, expectedKey, expectedValue);
    expectedKey = "table.iterator.scan.customcfcounter.opt.name1";
    expectedValue = "value1";
    checkTableForProperty(tops, tableName, expectedKey, expectedValue);
    expectedKey = "table.iterator.scan.customcfcounter.opt.name2";
    expectedValue = "value2";
    checkTableForProperty(tops, tableName, expectedKey, expectedValue);

    ts.exec("deletetable " + tableName, true);
    tableName = tableName + "1";

    ts.exec("createtable " + tableName, true);

    ts.input.set("\nname1 value1.1,value1.2,value1.3\nname2 value2\n\n");

    // Name on the CLI should override OptionDescriber (or user input name, in this case)
    ts.exec("setiter -scan -class org.apache.accumulo.core.iterators.ColumnFamilyCounter -p 30 -name cfcounter", true);
    expectedKey = "table.iterator.scan.cfcounter";
    expectedValue = "30,org.apache.accumulo.core.iterators.ColumnFamilyCounter";
    checkTableForProperty(tops, tableName, expectedKey, expectedValue);
    expectedKey = "table.iterator.scan.cfcounter.opt.name1";
    expectedValue = "value1.1,value1.2,value1.3";
    checkTableForProperty(tops, tableName, expectedKey, expectedValue);
    expectedKey = "table.iterator.scan.cfcounter.opt.name2";
    expectedValue = "value2";
    checkTableForProperty(tops, tableName, expectedKey, expectedValue);
  }

  protected void checkTableForProperty(TableOperations tops, String tableName, String expectedKey, String expectedValue) throws Exception {
    for (int i = 0; i < 5; i++) {
      for (Entry<String,String> entry : tops.getProperties(tableName)) {
        if (expectedKey.equals(entry.getKey())) {
          assertEquals(expectedValue, entry.getValue());
          return;
        }
      }
      Thread.sleep(500);
    }

    fail("Failed to find expected property on " + tableName + ": " + expectedKey + "=" + expectedValue);
  }

  @Test
  public void notable() throws Exception {
    final String table = name.getMethodName();

    // notable
    ts.exec("createtable " + table, true);
    ts.exec("scan", true, " " + table + ">", true);
    assertTrue(ts.output.get().contains(" " + table + ">"));
    ts.exec("notable", true);
    ts.exec("scan", false, "Not in a table context.", true);
    assertFalse(ts.output.get().contains(" " + table + ">"));
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void sleep() throws Exception {
    // sleep
    long now = System.currentTimeMillis();
    ts.exec("sleep 0.2", true);
    long diff = System.currentTimeMillis() - now;
    assertTrue("Diff was actually " + diff, diff >= 200);
    assertTrue("Diff was actually " + diff, diff < 600);
  }

  @Test
  public void addauths() throws Exception {
    final String table = name.getMethodName();
    // addauths
    ts.exec("createtable " + table + " -evc");
    boolean success = false;
    for (int i = 0; i < 9 && !success; i++) {
      try {
        ts.exec("insert a b c d -l foo", false, "does not have authorization", true, new ErrorMessageCallback() {
          @Override
          public String getErrorMessage() {
            try {
              Connector c = getConnector();
              return "Current auths for root are: " + c.securityOperations().getUserAuthorizations("root").toString();
            } catch (Exception e) {
              return "Could not check authorizations";
            }
          }
        });
      } catch (AssertionError e) {
        Thread.sleep(200);
      }
    }
    if (!success) {
      ts.exec("insert a b c d -l foo", false, "does not have authorization", true, new ErrorMessageCallback() {
        @Override
        public String getErrorMessage() {
          try {
            Connector c = getConnector();
            return "Current auths for root are: " + c.securityOperations().getUserAuthorizations("root").toString();
          } catch (Exception e) {
            return "Could not check authorizations";
          }
        }
      });
    }
    ts.exec("addauths -s foo,bar", true);
    boolean passed = false;
    for (int i = 0; i < 50 && !passed; i++) {
      try {
        ts.exec("getauths", true, "foo", true);
        ts.exec("getauths", true, "bar", true);
        passed = true;
      } catch (Exception e) {
        UtilWaitThread.sleep(300);
      }
    }
    assertTrue("Could not successfully see updated authoriations", passed);
    ts.exec("insert a b c d -l foo");
    ts.exec("scan", true, "[foo]");
    ts.exec("scan -s bar", true, "[foo]", false);
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void byeQuitExit() throws Exception {
    // bye, quit, exit
    for (String cmd : "bye quit exit".split(" ")) {
      assertFalse(ts.shell.getExit());
      ts.exec(cmd);
      assertTrue(ts.shell.getExit());
      ts.shell.setExit(false);
    }
  }

  @Test
  public void classpath() throws Exception {
    // classpath
    ts.exec("classpath", true, "Level 2: Java Classloader (loads everything defined by java classpath) URL classpath items are", true);
  }

  @Test
  public void clearCls() throws Exception {
    // clear/cls
    if (ts.shell.getReader().getTerminal().isAnsiSupported()) {
      ts.exec("cls", true, "[1;1H");
      ts.exec("clear", true, "[2J");
    } else {
      ts.exec("cls", false, "does not support");
      ts.exec("clear", false, "does not support");
    }
  }

  @Test
  public void clonetable() throws Exception {
    final String table = name.getMethodName(), clone = table + "_clone";

    // clonetable
    ts.exec("createtable " + table + " -evc");
    ts.exec("config -t " + table + " -s table.split.threshold=123M", true);
    ts.exec("addsplits -t " + table + " a b c", true);
    ts.exec("insert a b c value");
    ts.exec("scan", true, "value", true);
    ts.exec("clonetable " + table + " " + clone);
    // verify constraint, config, and splits were cloned
    ts.exec("constraint --list -t " + clone, true, "VisibilityConstraint=2", true);
    ts.exec("config -t " + clone + " -np", true, "123M", true);
    ts.exec("getsplits -t " + clone, true, "a\nb\nc\n");
    ts.exec("deletetable -f " + table);
    ts.exec("deletetable -f " + clone);
  }

  @Test
  public void testCompactions() throws Exception {
    final String table = name.getMethodName();

    // compact
    ts.exec("createtable " + table);

    String tableId = getTableId(table);

    // make two files
    ts.exec("insert a b c d");
    ts.exec("flush -w");
    ts.exec("insert x y z v");
    ts.exec("flush -w");
    int oldCount = countFiles(tableId);
    // merge two files into one
    ts.exec("compact -t " + table + " -w");
    assertTrue(countFiles(tableId) < oldCount);
    ts.exec("addsplits -t " + table + " f");
    // make two more files:
    ts.exec("insert m 1 2 3");
    ts.exec("flush -w");
    ts.exec("insert n 1 2 3");
    ts.exec("flush -w");
    List<String> oldFiles = getFiles(tableId);

    // at this point there are 4 files in the default tablet
    assertEquals("Files that were found: " + oldFiles, 4, oldFiles.size());

    // compact some data:
    ts.exec("compact -b g -e z -w");
    assertEquals(2, countFiles(tableId));
    ts.exec("compact -w");
    assertEquals(2, countFiles(tableId));
    ts.exec("merge --all -t " + table);
    ts.exec("compact -w");
    assertEquals(1, countFiles(tableId));
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void constraint() throws Exception {
    final String table = name.getMethodName();

    // constraint
    ts.exec("constraint -l -t " + MetadataTable.NAME + "", true, "MetadataConstraints=1", true);
    ts.exec("createtable " + table + " -evc");

    // Make sure the table is fully propagated through zoocache
    getTableId(table);

    ts.exec("constraint -l -t " + table, true, "VisibilityConstraint=2", true);
    ts.exec("constraint -t " + table + " -d 2", true, "Removed constraint 2 from table " + table);
    // wait for zookeeper updates to propagate
    UtilWaitThread.sleep(1000);
    ts.exec("constraint -l -t " + table, true, "VisibilityConstraint=2", false);
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void deletemany() throws Exception {
    final String table = name.getMethodName();

    // deletemany
    ts.exec("createtable " + table);
    make10();
    assertEquals(10, countkeys(table));
    ts.exec("deletemany -f -b row8");
    assertEquals(8, countkeys(table));
    ts.exec("scan -t " + table + " -np", true, "row8", false);
    make10();
    ts.exec("deletemany -f -b row4 -e row5");
    assertEquals(8, countkeys(table));
    make10();
    ts.exec("deletemany -f -c cf:col4,cf:col5");
    assertEquals(8, countkeys(table));
    make10();
    ts.exec("deletemany -f -r row3");
    assertEquals(9, countkeys(table));
    make10();
    ts.exec("deletemany -f -r row3");
    assertEquals(9, countkeys(table));
    make10();
    ts.exec("deletemany -f -b row3 -be -e row5 -ee");
    assertEquals(9, countkeys(table));
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void deleterows() throws Exception {
    final String table = name.getMethodName();

    ts.exec("createtable " + table);
    final String tableId = getTableId(table);

    // deleterows
    int base = countFiles(tableId);
    assertEquals(0, base);

    ts.exec("addsplits row5 row7");
    make10();
    ts.exec("flush -w -t " + table);
    // Might have some cruft here. Check a couple of times.
    List<String> files = null;
    boolean found = false;
    for (int i = 0; i < 50 && !found; i++) {
      files = getFiles(tableId);
      if (3 == files.size()) {
        found = true;
      } else {
        UtilWaitThread.sleep(300);
      }
    }
    assertNotNull(files);
    assertEquals("Found the following files: " + files, 3, files.size());
    ts.exec("deleterows -t " + table + " -b row5 -e row7", true);
    assertEquals(2, countFiles(tableId));
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void groups() throws Exception {
    final String table = name.getMethodName();

    ts.exec("createtable " + table);
    ts.exec("setgroups -t " + table + " alpha=a,b,c num=3,2,1");
    ts.exec("getgroups -t " + table, true, "alpha=a,b,c", true);
    ts.exec("getgroups -t " + table, true, "num=1,2,3", true);
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void grep() throws Exception {
    final String table = name.getMethodName();

    ts.exec("createtable " + table, true);
    make10();
    ts.exec("grep row[123]", true, "row1", false);
    ts.exec("grep row5", true, "row5", true);
    ts.exec("deletetable -f " + table, true);
  }

  @Test
  public void help() throws Exception {
    ts.exec("help -np", true, "Help Commands", true);
    ts.exec("?", true, "Help Commands", true);
    for (String c : ("bye exit quit " + "about help info ? " + "deleteiter deletescaniter listiter setiter setscaniter "
        + "grant revoke systempermissions tablepermissions userpermissions " + "execfile history " + "authenticate cls clear notable sleep table user whoami "
        + "clonetable config createtable deletetable droptable du exporttable importtable offline online renametable tables "
        + "addsplits compact constraint flush getgropus getsplits merge setgroups " + "addauths createuser deleteuser dropuser getauths passwd setauths users "
        + "delete deletemany deleterows egrep formatter interpreter grep importdirectory insert maxrow scan").split(" ")) {
      ts.exec("help " + c, true);
    }
  }

  // @Test(timeout = 45000)
  public void history() throws Exception {
    final String table = name.getMethodName();

    ts.exec("history -c", true);
    ts.exec("createtable " + table);
    ts.exec("deletetable -f " + table);
    ts.exec("history", true, table, true);
    ts.exec("history", true, "history", true);
  }

  @Test
  public void importDirectory() throws Exception {
    final String table = name.getMethodName();

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    File importDir = new File(rootPath, "import");
    importDir.mkdir();
    String even = new File(importDir, "even.rf").toString();
    String odd = new File(importDir, "odd.rf").toString();
    File errorsDir = new File(rootPath, "errors");
    errorsDir.mkdir();
    fs.mkdirs(new Path(errorsDir.toString()));
    AccumuloConfiguration aconf = AccumuloConfiguration.getDefaultConfiguration();
    FileSKVWriter evenWriter = FileOperations.getInstance().openWriter(even, fs, conf, aconf);
    evenWriter.startDefaultLocalityGroup();
    FileSKVWriter oddWriter = FileOperations.getInstance().openWriter(odd, fs, conf, aconf);
    oddWriter.startDefaultLocalityGroup();
    long timestamp = System.currentTimeMillis();
    Text cf = new Text("cf");
    Text cq = new Text("cq");
    Value value = new Value("value".getBytes());
    for (int i = 0; i < 100; i += 2) {
      Key key = new Key(new Text(String.format("%8d", i)), cf, cq, timestamp);
      evenWriter.append(key, value);
      key = new Key(new Text(String.format("%8d", i + 1)), cf, cq, timestamp);
      oddWriter.append(key, value);
    }
    evenWriter.close();
    oddWriter.close();
    assertEquals(0, ts.shell.getExitCode());
    ts.exec("createtable " + table, true);
    ts.exec("importdirectory " + importDir + " " + errorsDir + " true", true);
    ts.exec("scan -r 00000000", true, "00000000", true);
    ts.exec("scan -r 00000099", true, "00000099", true);
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void info() throws Exception {
    ts.exec("info", true, Constants.VERSION, true);
  }

  @Test
  public void interpreter() throws Exception {
    final String table = name.getMethodName();

    ts.exec("createtable " + table, true);
    ts.exec("interpreter -l", true, "HexScan", false);
    ts.exec("insert \\x02 cf cq value", true);
    ts.exec("scan -b 02", true, "value", false);
    ts.exec("interpreter -i org.apache.accumulo.core.util.interpret.HexScanInterpreter", true);
    // Need to allow time for this to propagate through zoocache/zookeeper
    UtilWaitThread.sleep(3000);

    ts.exec("interpreter -l", true, "HexScan", true);
    ts.exec("scan -b 02", true, "value", true);
    ts.exec("deletetable -f " + table, true);
  }

  @Test
  public void listcompactions() throws Exception {
    final String table = name.getMethodName();

    ts.exec("createtable " + table, true);
    ts.exec("config -t " + table + " -s table.iterator.minc.slow=30,org.apache.accumulo.test.functional.SlowIterator", true);
    ts.exec("config -t " + table + " -s table.iterator.minc.slow.opt.sleepTime=1000", true);
    ts.exec("insert a cf cq value", true);
    ts.exec("insert b cf cq value", true);
    ts.exec("insert c cf cq value", true);
    ts.exec("insert d cf cq value", true);
    ts.exec("flush -t " + table, true);
    ts.exec("sleep 0.2", true);
    ts.exec("listcompactions", true, "default_tablet");
    String[] lines = ts.output.get().split("\n");
    String last = lines[lines.length - 1];
    String[] parts = last.split("\\|");
    assertEquals(12, parts.length);
    ts.exec("deletetable -f " + table, true);
  }

  @Test
  public void maxrow() throws Exception {
    final String table = name.getMethodName();

    ts.exec("createtable " + table, true);
    ts.exec("insert a cf cq value", true);
    ts.exec("insert b cf cq value", true);
    ts.exec("insert ccc cf cq value", true);
    ts.exec("insert zzz cf cq value", true);
    ts.exec("maxrow", true, "zzz", true);
    ts.exec("delete zzz cf cq", true);
    ts.exec("maxrow", true, "ccc", true);
    ts.exec("deletetable -f " + table, true);
  }

  @Test
  public void merge() throws Exception {
    final String table = name.getMethodName();

    ts.exec("createtable " + table);
    ts.exec("addsplits a m z");
    ts.exec("getsplits", true, "z", true);
    ts.exec("merge --all", true);
    ts.exec("getsplits", true, "z", false);
    ts.exec("deletetable -f " + table);
    ts.exec("getsplits -t " + MetadataTable.NAME + "", true);
    assertEquals(2, ts.output.get().split("\n").length);
    ts.exec("getsplits -t accumulo.root", true);
    assertEquals(1, ts.output.get().split("\n").length);
    ts.exec("merge --all -t " + MetadataTable.NAME + "");
    ts.exec("getsplits -t " + MetadataTable.NAME + "", true);
    assertEquals(1, ts.output.get().split("\n").length);
  }

  @Test
  public void ping() throws Exception {
    for (int i = 0; i < 10; i++) {
      ts.exec("ping", true, "OK", true);
      // wait for both tservers to start up
      if (ts.output.get().split("\n").length == 3)
        break;
      UtilWaitThread.sleep(1000);

    }
    assertEquals(3, ts.output.get().split("\n").length);
  }

  @Test
  public void renametable() throws Exception {
    final String table = name.getMethodName() + "1", rename = name.getMethodName() + "2";

    ts.exec("createtable " + table);
    ts.exec("insert this is a value");
    ts.exec("renametable " + table + " " + rename);
    ts.exec("tables", true, rename, true);
    ts.exec("tables", true, table, false);
    ts.exec("scan -t " + rename, true, "value", true);
    ts.exec("deletetable -f " + rename, true);
  }

  @Test
  public void tables() throws Exception {
    final String table = name.getMethodName(), table1 = table + "_z", table2 = table + "_a";
    ts.exec("createtable " + table1);
    ts.exec("createtable " + table2);
    ts.exec("notable");
    String lst = ts.exec("tables -l");
    assertTrue(lst.indexOf(table2) < lst.indexOf(table1));
    lst = ts.exec("tables -l -s");
    assertTrue(lst.indexOf(table1) < lst.indexOf(table2));
  }

  @Test
  public void systempermission() throws Exception {
    ts.exec("systempermissions");
    assertEquals(11, ts.output.get().split("\n").length - 1);
    ts.exec("tablepermissions", true);
    assertEquals(6, ts.output.get().split("\n").length - 1);
  }

  @Test
  public void listscans() throws Exception {
    final String table = name.getMethodName();

    ts.exec("createtable " + table, true);

    // Should be about a 3 second scan
    for (int i = 0; i < 6; i++) {
      ts.exec("insert " + i + " cf cq value", true);
    }
    Connector connector = getConnector();
    final Scanner s = connector.createScanner(table, Authorizations.EMPTY);
    IteratorSetting cfg = new IteratorSetting(30, SlowIterator.class);
    SlowIterator.setSleepTime(cfg, 500);
    s.addScanIterator(cfg);

    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          FunctionalTestUtils.count(s);
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    };
    thread.start();

    List<String> scans = new ArrayList<String>();
    // Try to find the active scan for about 15seconds
    for (int i = 0; i < 50 && scans.isEmpty(); i++) {
      String currentScans = ts.exec("listscans", true);
      log.info("Got output from listscans:\n" + currentScans);
      String[] lines = currentScans.split("\n");
      for (int scanOffset = 2; scanOffset < lines.length; scanOffset++) {
        String currentScan = lines[scanOffset];
        if (currentScan.contains(table)) {
          log.info("Retaining scan: " + currentScan);
          scans.add(currentScan);
        } else {
          log.info("Ignoring scan because of wrong table: " + currentScan);
        }
      }
      UtilWaitThread.sleep(300);
    }
    thread.join();

    assertFalse("Could not find any active scans over table " + table, scans.isEmpty());

    for (String scan : scans) {
      if (!scan.contains("RUNNING")) {
        log.info("Ignoring scan because it doesn't contain 'RUNNING': " + scan);
        continue;
      }
      String parts[] = scan.split("\\|");
      assertEquals("Expected 13 colums, but found " + parts.length + " instead for '" + Arrays.toString(parts) + "'", 13, parts.length);
      String tserver = parts[0].trim();
      // TODO: any way to tell if the client address is accurate? could be local IP, host, loopback...?
      String hostPortPattern = ".+:\\d+";
      assertTrue(tserver.matches(hostPortPattern));
      assertTrue(getConnector().instanceOperations().getTabletServers().contains(tserver));
      String client = parts[1].trim();
      assertTrue(client.matches(hostPortPattern));
    }

    ts.exec("deletetable -f " + table, true);
  }

  @Test
  public void testPertableClasspath() throws Exception {
    final String table = name.getMethodName();

    File fooFilterJar = File.createTempFile("FooFilter", ".jar", new File(rootPath));

    FileUtils.copyURLToFile(this.getClass().getResource("/FooFilter.jar"), fooFilterJar);
    fooFilterJar.deleteOnExit();

    File fooConstraintJar = File.createTempFile("FooConstraint", ".jar", new File(rootPath));
    FileUtils.copyURLToFile(this.getClass().getResource("/FooConstraint.jar"), fooConstraintJar);
    fooConstraintJar.deleteOnExit();

    ts.exec("config -s " + Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1=" + fooFilterJar.toURI().toString() + ","
        + fooConstraintJar.toURI().toString(), true);

    ts.exec("createtable " + table, true);
    ts.exec("config -t " + table + " -s " + Property.TABLE_CLASSPATH.getKey() + "=cx1", true);

    UtilWaitThread.sleep(200);

    // We can't use the setiter command as Filter implements OptionDescriber which
    // forces us to enter more input that I don't know how to input
    // Instead, we can just manually set the property on the table.
    ts.exec("config -t " + table + " -s " + Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.foo=10,org.apache.accumulo.test.FooFilter");

    ts.exec("insert foo f q v", true);

    UtilWaitThread.sleep(100);

    ts.exec("scan -np", true, "foo", false);

    ts.exec("constraint -a FooConstraint", true);

    ts.exec("offline -w " + table);
    ts.exec("online -w " + table);

    ts.exec("table " + table, true);
    ts.exec("insert foo f q v", false);
    ts.exec("insert ok foo q v", true);

    ts.exec("deletetable -f " + table, true);
    ts.exec("config -d " + Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1");

  }

  @Test
  public void trace() throws Exception {
    // Make sure to not collide with the "trace" table
    final String table = name.getMethodName() + "Test";

    ts.exec("trace on", true);
    ts.exec("createtable " + table, true);
    ts.exec("insert a b c value", true);
    ts.exec("scan -np", true, "value", true);
    ts.exec("deletetable -f " + table);
    ts.exec("sleep 1");
    String trace = ts.exec("trace off");
    System.out.println(trace);
    assertTrue(trace.contains("sendMutations"));
    assertTrue(trace.contains("startScan"));
    assertTrue(trace.contains("DeleteTable"));
  }

  @Test
  public void badLogin() throws Exception {
    ts.input.set(getRootPassword() + "\n");
    String err = ts.exec("user NoSuchUser", false);
    assertTrue(err.contains("BAD_CREDENTIALS for user NoSuchUser"));
  }

  @Test
  public void namespaces() throws Exception {
    ts.exec("namespaces", true, "\"\"", true); // default namespace, displayed as quoted empty string
    ts.exec("namespaces", true, Namespaces.ACCUMULO_NAMESPACE, true);
    ts.exec("createnamespace thing1", true);
    String namespaces = ts.exec("namespaces");
    assertTrue(namespaces.contains("thing1"));

    ts.exec("renamenamespace thing1 thing2");
    namespaces = ts.exec("namespaces");
    assertTrue(namespaces.contains("thing2"));
    assertTrue(!namespaces.contains("thing1"));

    // can't delete a namespace that still contains tables, unless you do -f
    ts.exec("createtable thing2.thingy", true);
    ts.exec("deletenamespace thing2");
    ts.exec("y");
    ts.exec("namespaces", true, "thing2", true);

    ts.exec("du -ns thing2", true, "thing2.thingy", true);

    // all "TableOperation" commands can take a namespace
    ts.exec("offline -ns thing2", true);
    ts.exec("online -ns thing2", true);
    ts.exec("flush -ns thing2", true);
    ts.exec("compact -ns thing2", true);
    ts.exec("createnamespace testers3", true);
    ts.exec("createtable testers3.1", true);
    ts.exec("createtable testers3.2", true);
    ts.exec("deletetable -ns testers3 -f", true);
    ts.exec("tables", true, "testers3.1", false);
    ts.exec("namespaces", true, "testers3", true);
    ts.exec("deletenamespace testers3 -f", true);
    ts.input.set("true\n\n\nSTRING\n");
    ts.exec("setiter -ns thing2 -scan -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -n name", true);
    ts.exec("listiter -ns thing2 -scan", true, "Summing", true);
    ts.exec("deleteiter -ns thing2 -n name -scan", true);
    ts.exec("createuser dude");
    ts.exec("pass");
    ts.exec("pass");
    ts.exec("grant Namespace.CREATE_TABLE -ns thing2 -u dude", true);
    ts.exec("revoke Namespace.CREATE_TABLE -ns thing2 -u dude", true);

    // properties override and such
    ts.exec("config -ns thing2 -s table.file.max=44444", true);
    ts.exec("config -ns thing2", true, "44444", true);
    ts.exec("config -t thing2.thingy", true, "44444", true);
    ts.exec("config -t thing2.thingy -s table.file.max=55555", true);
    ts.exec("config -t thing2.thingy", true, "55555", true);

    // can copy properties when creating
    ts.exec("createnamespace thing3 -cc thing2", true);
    ts.exec("config -ns thing3", true, "44444", true);

    ts.exec("deletenamespace -f thing2", true);
    ts.exec("namespaces", true, "thing2", false);
    ts.exec("tables", true, "thing2.thingy", false);

    // put constraints on a namespace
    ts.exec("constraint -ns thing3 -a org.apache.accumulo.examples.simple.constraints.NumericValueConstraint", true);
    ts.exec("createtable thing3.constrained", true);
    ts.exec("table thing3.constrained", true);
    ts.exec("constraint -d 1");
    // should fail
    ts.exec("constraint -l", true, "NumericValueConstraint", true);
    ts.exec("insert r cf cq abc", false);
    ts.exec("constraint -ns thing3 -d 1");
    ts.exec("sleep 1");
    ts.exec("insert r cf cq abc", true);
  }

  private int countkeys(String table) throws IOException {
    ts.exec("scan -np -t " + table);
    return ts.output.get().split("\n").length - 1;
  }

  @Test
  public void scans() throws Exception {
    ts.exec("createtable t");
    make10();
    String result = ts.exec("scan -np -b row1 -e row1");
    assertEquals(2, result.split("\n").length);
    result = ts.exec("scan -np -b row3 -e row5");
    assertEquals(4, result.split("\n").length);
    result = ts.exec("scan -np -r row3");
    assertEquals(2, result.split("\n").length);
    result = ts.exec("scan -np -b row:");
    assertEquals(1, result.split("\n").length);
    result = ts.exec("scan -np -b row");
    assertEquals(11, result.split("\n").length);
    result = ts.exec("scan -np -e row:");
    assertEquals(11, result.split("\n").length);
    ts.exec("deletetable -f t");
  }

  @Test
  public void whoami() throws Exception {
    assertTrue(ts.exec("whoami", true).contains("root"));
    ts.input.set("secret\nsecret\n");
    ts.exec("createuser test_user");
    ts.exec("setauths -u test_user -s 12,3,4");
    String auths = ts.exec("getauths -u test_user");
    assertTrue(auths.contains("3") && auths.contains("12") && auths.contains("4"));
    ts.input.set("secret\n");
    ts.exec("user test_user", true);
    assertTrue(ts.exec("whoami", true).contains("test_user"));
    ts.input.set(getRootPassword() + "\n");
    ts.exec("user root", true);
  }

  private void make10() throws IOException {
    for (int i = 0; i < 10; i++) {
      ts.exec(String.format("insert row%d cf col%d value", i, i));
    }
  }

  private List<String> getFiles(String tableId) throws IOException {
    ts.output.clear();

    ts.exec("scan -t " + MetadataTable.NAME + " -np -c file -b " + tableId + " -e " + tableId + "~");

    log.debug("countFiles(): " + ts.output.get());

    String[] lines = StringUtils.split(ts.output.get(), "\n");
    ts.output.clear();

    if (0 == lines.length) {
      return Collections.emptyList();
    }

    return Arrays.asList(Arrays.copyOfRange(lines, 1, lines.length));
  }

  private int countFiles(String tableId) throws IOException {
    return getFiles(tableId).size();
  }

  private String getTableId(String tableName) throws Exception {
    Connector conn = getConnector();

    for (int i = 0; i < 5; i++) {
      Map<String,String> nameToId = conn.tableOperations().tableIdMap();
      if (nameToId.containsKey(tableName)) {
        return nameToId.get(tableName);
      } else {
        Thread.sleep(1000);
      }
    }

    fail("Could not find ID for table: " + tableName);
    // Will never get here
    return null;
  }

}
