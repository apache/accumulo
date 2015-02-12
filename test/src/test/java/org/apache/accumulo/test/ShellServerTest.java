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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jline.ConsoleReader;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.server.trace.TraceServer;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.DistCp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class ShellServerTest {
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

  private static abstract class ErrorMessageCallback {
    public abstract String getErrorMessage();
  }

  private static class NoOpErrorMessageCallback extends ErrorMessageCallback {
    private static final String empty = "";

    public String getErrorMessage() {
      return empty;
    }
  }

  private static final NoOpErrorMessageCallback noop = new NoOpErrorMessageCallback();

  private static String secret = "superSecret";
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  public TestOutputStream output;
  public Shell shell;
  private static Process traceProcess;

  @Rule
  public TestName name = new TestName();

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

  void assertGoodExit(String s, boolean stringPresent, ErrorMessageCallback callback) {
    Shell.log.debug(output.get());
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

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), secret);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();

    System.setProperty("HOME", folder.getRoot().getAbsolutePath());

    // use reflection to call this method so it does not need to be made public
    Method method = cluster.getClass().getDeclaredMethod("exec", Class.class, String[].class);
    method.setAccessible(true);
    traceProcess = (Process) method.invoke(cluster, TraceServer.class, new String[0]);

    // give the tracer some time to start
    UtilWaitThread.sleep(1000);
  }

  @Before
  public void setupShell() throws Exception {
    // start the shell
    output = new TestOutputStream();
    PrintWriter pw = new PrintWriter(new OutputStreamWriter(output));
    shell = new Shell(new ConsoleReader(new FileInputStream(FileDescriptor.in), new OutputStreamWriter(output)), pw);
    shell.setLogErrorsToConsole();
    shell.config("-u", "root", "-p", secret, "-z", cluster.getInstanceName(), cluster.getZooKeepers());
    exec("quit", true);
    shell.start();
    shell.setExit(false);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    traceProcess.destroy();
    folder.delete();
  }

  @Test(timeout = 60000)
  public void exporttableImporttable() throws Exception {
    final String table = name.getMethodName(), table2 = table + "2";

    // exporttable / importtable
    exec("createtable " + table + " -evc", true);
    make10();
    exec("addsplits row5", true);
    exec("config -t " + table + " -s table.split.threshold=345M", true);
    exec("offline " + table, true);
    String export = folder.newFolder().toString();
    exec("exporttable -t " + table + " " + export, true);
    DistCp cp = newDistCp();
    String import_ = folder.newFolder().toString();
    cp.run(new String[] {"-f", export + "/distcp.txt", import_});
    exec("importtable " + table2 + " " + import_, true);
    exec("config -t " + table2 + " -np", true, "345M", true);
    exec("getsplits -t " + table2, true, "row5", true);
    exec("constraint --list -t " + table2, true, "VisibilityConstraint=1", true);
    exec("onlinetable " + table, true);
    exec("deletetable -f " + table, true);
    exec("deletetable -f " + table2, true);
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

  @Test(timeout = 45000)
  public void setscaniterDeletescaniter() throws Exception {
    final String table = name.getMethodName();

    // setscaniter, deletescaniter
    exec("createtable " + table);
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    shell.getReader().setInput(new ByteArrayInputStream("true\n\n\nSTRING\n".getBytes()));
    exec("setscaniter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -n name", true);
    exec("scan", true, "3", true);
    exec("deletescaniter -n name", true);
    exec("scan", true, "1", true);
    exec("deletetable -f " + table);

  }

  @Test(timeout = 45000)
  public void execfile() throws Exception {
    // execfile
    File file = folder.newFile();
    PrintWriter writer = new PrintWriter(file.getAbsolutePath());
    writer.println("about");
    writer.close();
    exec("execfile " + file.getAbsolutePath(), true, Constants.VERSION, true);

  }

  @Test(timeout = 45000)
  public void egrep() throws Exception {
    final String table = name.getMethodName();

    // egrep
    exec("createtable " + table);
    make10();
    String lines = exec("egrep row[123]", true);
    assertTrue(lines.split("\n").length - 1 == 3);
    exec("deletetable -f " + table);
  }

  @Test(timeout = 45000)
  public void du() throws Exception {
    final String table = name.getMethodName();

    // create and delete a table so we get out of a table context in the shell
    exec("notable", true);

    // Calling du not in a table context shouldn't throw an error
    output.clear();
    exec("du", true, "", true);

    output.clear();
    exec("createtable " + table);
    make10();
    exec("flush -t " + table + " -w");
    exec("du " + table, true, " [" + table + "]", true);
    output.clear();
    shell.execCommand("du -h", false, false);
    String o = output.get();
    // for some reason, there's a bit of fluctuation
    assertTrue("Output did not match regex: '" + o + "'", o.matches(".*2[6-7][0-9]B\\s\\[" + table + "\\]\\n"));
    exec("deletetable -f " + table);
  }

  @Test(timeout = 1000)
  public void debug() throws Exception {
    exec("debug", true, "off", true);
    exec("debug on", true);
    exec("debug", true, "on", true);
    exec("debug off", true);
    exec("debug", true, "off", true);
    exec("debug debug", false);
    exec("debug debug debug", false);
  }

  @Test(timeout = 45000)
  public void user() throws Exception {
    final String table = name.getMethodName();

    // createuser, deleteuser, user, users, droptable, grant, revoke
    shell.getReader().setInput(new ByteArrayInputStream("secret\nsecret\n".getBytes()));
    exec("createuser xyzzy", true);
    exec("users", true, "xyzzy", true);
    String perms = exec("userpermissions -u xyzzy", true);
    assertTrue(perms.contains("Table permissions (!METADATA): Table.READ"));
    exec("grant -u xyzzy -s System.CREATE_TABLE", true);
    perms = exec("userpermissions -u xyzzy", true);
    assertTrue(perms.contains(""));
    exec("grant -u root -t !METADATA Table.WRITE", true);
    exec("grant -u root -t !METADATA Table.GOOFY", false);
    exec("grant -u root -s foo", false);
    exec("grant -u xyzzy -t !METADATA foo", false);
    shell.getReader().setInput(new ByteArrayInputStream("secret\nsecret\n".getBytes()));
    exec("user xyzzy", true);
    exec("createtable " + table, true, "xyzzy@", true);
    exec("insert row1 cf cq 1", true);
    exec("scan", true, "row1", true);
    exec("droptable -f " + table, true);
    exec("deleteuser xyzzy", false, "delete yourself", true);
    shell.getReader().setInput(new ByteArrayInputStream((secret + "\n" + secret + "\n").getBytes()));
    exec("user root", true);
    exec("revoke -u xyzzy -s System.CREATE_TABLE", true);
    exec("revoke -u xyzzy -s System.GOOFY", false);
    exec("revoke -u xyzzy -s foo", false);
    exec("revoke -u xyzzy -t !METADATA Table.WRITE", true);
    exec("revoke -u xyzzy -t !METADATA Table.GOOFY", false);
    exec("revoke -u xyzzy -t !METADATA foo", false);
    exec("deleteuser xyzzy", true);
    exec("users", true, "xyzzy", false);
  }

  @Test(timeout = 45000)
  public void iter() throws Exception {
    final String table = name.getMethodName();

    // setshelliter, listshelliter, deleteshelliter
    exec("createtable " + table);
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    shell.getReader().setInput(new ByteArrayInputStream("true\n\n\nSTRING\n".getBytes()));
    exec("setshelliter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -pn sum -n name", true);
    shell.getReader().setInput(new ByteArrayInputStream("true\n\n\nSTRING\n".getBytes()));
    exec("setshelliter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 11 -pn sum -n xyzzy", true);
    exec("scan -pn sum", true, "3", true);
    exec("listshelliter", true, "Iterator name", true);
    exec("listshelliter", true, "Iterator xyzzy", true);
    exec("listshelliter", true, "Profile : sum", true);
    exec("deleteshelliter -pn sum -n name", true);
    exec("listshelliter", true, "Iterator name", false);
    exec("listshelliter", true, "Iterator xyzzy", true);
    exec("deleteshelliter -pn sum -a", true);
    exec("listshelliter", true, "Iterator xyzzy", false);
    exec("listshelliter", true, "Profile : sum", false);
    exec("deletetable -f " + table);
    // list iter
    exec("createtable " + table);
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    shell.getReader().setInput(new ByteArrayInputStream("true\n\n\nSTRING\n".getBytes()));
    exec("setiter -scan -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -n name", true);
    shell.getReader().setInput(new ByteArrayInputStream("true\n\n\nSTRING\n".getBytes()));
    exec("setiter -scan -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 11 -n xyzzy", true);
    exec("scan", true, "3", true);
    exec("listiter -scan", true, "Iterator name", true);
    exec("listiter -scan", true, "Iterator xyzzy", true);
    exec("listiter -minc", true, "Iterator name", false);
    exec("listiter -minc", true, "Iterator xyzzy", false);
    exec("deleteiter -scan -n name", true);
    exec("listiter -scan", true, "Iterator name", false);
    exec("listiter -scan", true, "Iterator xyzzy", true);
    exec("deletetable -f " + table);

  }

  @Test(timeout = 45000)
  public void notable() throws Exception {
    final String table = name.getMethodName();

    // notable
    exec("createtable " + table, true);
    exec("scan", true, " " + table + ">", true);
    assertTrue(output.get().contains(" " + table + ">"));
    exec("notable", true);
    exec("scan", false, "Not in a table context.", true);
    assertFalse(output.get().contains(" " + table + ">"));
    exec("deletetable -f " + table);
  }

  @Test(timeout = 45000)
  public void sleep() throws Exception {
    // sleep
    long now = System.currentTimeMillis();
    exec("sleep 0.2", true);
    long diff = System.currentTimeMillis() - now;
    assertTrue("Diff was actually " + diff, diff >= 200);
    assertTrue("Diff was actually " + diff, diff < 600);
  }

  @Test(timeout = 45000)
  public void addauths() throws Exception {
    final String table = name.getMethodName();
    // addauths
    exec("createtable " + table + " -evc");
    boolean success = false;
    // TabletServer hosting this table must see the constraint update before insert will fail properly
    for (int i = 0; i < 9 && !success; i++) {
      try {
        exec("insert a b c d -l foo", false, "does not have authorization", true, new ErrorMessageCallback() {
          public String getErrorMessage() {
            try {
              Connector c = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers()).getConnector("root", new PasswordToken(secret));
              return "Current auths for root are: " + c.securityOperations().getUserAuthorizations("root").toString();
            } catch (Exception e) {
              return "Could not check authorizations";
            }
          }
        });
        success = true;
      } catch (AssertionError e) {
        Thread.sleep(200);
      }
    }
    // If we still couldn't do it, try again and let it fail
    if (!success) {
      exec("insert a b c d -l foo", false, "does not have authorization", true, new ErrorMessageCallback() {
        public String getErrorMessage() {
          try {
            Connector c = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers()).getConnector("root", new PasswordToken(secret));
            return "Current auths for root are: " + c.securityOperations().getUserAuthorizations("root").toString();
          } catch (Exception e) {
            return "Could not check authorizations";
          }
        }
      });
    }
    exec("addauths -s foo,bar", true);
    boolean passed = false;
    for (int i = 0; i < 50 && !passed; i++) {
      try {
        exec("getauths", true, "foo", true);
        exec("getauths", true, "bar", true);
        passed = true;
      } catch (Exception e) {
        UtilWaitThread.sleep(300);
      }
    }
    Assert.assertTrue("Could not successfully see updated authoriations", passed);
    exec("insert a b c d -l foo");
    exec("scan", true, "[foo]");
    exec("scan -s bar", true, "[foo]", false);
    exec("deletetable -f " + table);
  }

  @Test(timeout = 45000)
  public void byeQuitExit() throws Exception {
    // bye, quit, exit
    for (String cmd : "bye quit exit".split(" ")) {
      assertFalse(shell.getExit());
      exec(cmd);
      assertTrue(shell.getExit());
      shell.setExit(false);
    }
  }

  @Test(timeout = 45000)
  public void classpath() throws Exception {
    // classpath
    exec("classpath", true, "Level 2: Java Classloader (loads everything defined by java classpath) URL classpath items are", true);
  }

  @Test(timeout = 45000)
  public void clearCls() throws Exception {
    // clear/cls
    exec("cls", true, "[1;1H");
    exec("clear", true, "[2J");
  }

  @Test(timeout = 45000)
  public void clonetable() throws Exception {
    final String table = name.getMethodName(), clone = table + "_clone";

    // clonetable
    exec("createtable " + table + " -evc");
    exec("config -t " + table + " -s table.split.threshold=123M", true);
    exec("addsplits -t " + table + " a b c", true);
    exec("insert a b c value");
    exec("scan", true, "value", true);
    exec("clonetable " + table + " " + clone);
    // verify constraint, config, and splits were cloned
    exec("constraint --list -t " + clone, true, "VisibilityConstraint=1", true);
    exec("config -t " + clone + " -np", true, "123M", true);
    exec("getsplits -t " + clone, true, "a\nb\nc\n");
    exec("deletetable -f " + table);
    exec("deletetable -f " + clone);
  }

  @Test(timeout = 45000)
  public void splitMerge() throws Exception {
    final String table = name.getMethodName();

    // compact
    exec("createtable " + table);

    String tableId = getTableId(table);

    // make two files
    exec("insert a b c d");
    exec("flush -w");
    exec("insert x y z v");
    exec("flush -w");
    int oldCount = countFiles(tableId);
    // merge two files into one
    exec("compact -t " + table + " -w");
    assertTrue(countFiles(tableId) < oldCount);
    exec("addsplits -t " + table + " f");
    // make two more files:
    exec("insert m 1 2 3");
    exec("flush -w");
    exec("insert n 1 2 3");
    exec("flush -w");
    List<String> oldFiles = getFiles(tableId);

    // at this point there are 4 files in the default tablet
    assertEquals("Files that were found: " + oldFiles, 4, oldFiles.size());

    // compact some data:
    exec("compact -b g -e z -w");
    assertEquals(2, countFiles(tableId));
    exec("compact -w");
    assertEquals(2, countFiles(tableId));
    exec("merge --all -t " + table);
    exec("compact -w");
    assertEquals(1, countFiles(tableId));
    exec("deletetable -f " + table);
  }

  @Test(timeout = 45000)
  public void constraint() throws Exception {
    final String table = name.getMethodName();

    // constraint
    exec("constraint -l -t !METADATA", true, "MetadataConstraints=1", true);
    exec("createtable " + table + " -evc");

    // Make sure the table is fully propagated through zoocache
    getTableId(table);

    exec("constraint -l -t " + table, true, "VisibilityConstraint=1", true);
    Thread.sleep(250);
    exec("constraint -t " + table + " -d 1", true, "Removed constraint 1 from table c");
    Thread.sleep(250);
    exec("constraint -l -t " + table, true, "VisibilityConstraint=1", false);
    exec("deletetable -f " + table);
  }

  @Test(timeout = 45000)
  public void deletemany() throws Exception {
    final String table = name.getMethodName();

    // deletemany
    exec("createtable " + table);
    make10();
    assertEquals(10, countkeys(table));
    exec("deletemany -f -b row8");
    assertEquals(8, countkeys(table));
    exec("scan -t " + table + " -np", true, "row8", false);
    make10();
    exec("deletemany -f -b row4 -e row5");
    assertEquals(8, countkeys(table));
    make10();
    exec("deletemany -f -c cf:col4,cf:col5");
    assertEquals(8, countkeys(table));
    make10();
    exec("deletemany -f -r row3");
    assertEquals(9, countkeys(table));
    make10();
    exec("deletemany -f -r row3");
    assertEquals(9, countkeys(table));
    make10();
    exec("deletemany -f -b row3 -be -e row5 -ee");
    assertEquals(9, countkeys(table));
    exec("deletetable -f " + table);
  }

  @Test(timeout = 45000)
  public void deleterows() throws Exception {
    final String table = name.getMethodName();

    exec("createtable " + table);
    final String tableId = getTableId(table);

    // deleterows
    int base = countFiles(tableId);
    assertEquals(0, base);

    exec("addsplits row5 row7");
    make10();
    exec("flush -w -t " + table);
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
    exec("deleterows -t " + table + " -b row5 -e row7", true);
    assertEquals(2, countFiles(tableId));
    exec("deletetable -f " + table);
  }

  @Test(timeout = 45000)
  public void groups() throws Exception {
    final String table = name.getMethodName();

    exec("createtable " + table);
    exec("setgroups -t " + table + " alpha=a,b,c num=3,2,1");
    exec("getgroups -t " + table, true, "alpha=a,b,c", true);
    exec("getgroups -t " + table, true, "num=1,2,3", true);
    exec("deletetable -f " + table);
  }

  @Test(timeout = 45000)
  public void grep() throws Exception {
    final String table = name.getMethodName();

    exec("createtable " + table, true);
    make10();
    exec("grep row[123]", true, "row1", false);
    exec("grep row5", true, "row5", true);
    exec("deletetable -f " + table, true);
  }

  @Test(timeout = 45000)
  public void help() throws Exception {
    exec("help -np", true, "Help Commands", true);
    shell.getReader().setInput(new ByteArrayInputStream("\n\n".getBytes()));
    exec("?", true, "Help Commands", true);
    for (String c : ("bye exit quit " + "about help info ? " + "deleteiter deletescaniter listiter setiter setscaniter "
        + "grant revoke systempermissions tablepermissions userpermissions " + "execfile history " + "authenticate cls clear notable sleep table user whoami "
        + "clonetable config createtable deletetable droptable du exporttable importtable offline online renametable tables "
        + "addsplits compact constraint flush getgropus getsplits merge setgroups " + "addauths createuser deleteuser dropuser getauths passwd setauths users "
        + "delete deletemany deleterows egrep formatter interpreter grep importdirectory insert maxrow scan").split(" ")) {
      exec("help " + c, true);
    }
  }

  // @Test(timeout = 45000)
  public void history() throws Exception {
    final String table = name.getMethodName();

    exec("history -c", true);
    exec("createtable " + table);
    exec("deletetable -f " + table);
    exec("history", true, table, true);
    exec("history", true, "history", true);
  }

  @Test(timeout = 45000)
  public void importDirectory() throws Exception {
    final String table = name.getMethodName();

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    File importDir = folder.newFolder("import");
    String even = new File(importDir, "even.rf").toString();
    String odd = new File(importDir, "odd.rf").toString();
    File errorsDir = folder.newFolder("errors");
    fs.mkdirs(new Path(errorsDir.toString()));
    AccumuloConfiguration aconf = AccumuloConfiguration.getDefaultConfiguration();
    FileSKVWriter evenWriter = FileOperations.getInstance().openWriter(even, fs, conf, aconf);
    evenWriter.startDefaultLocalityGroup();
    FileSKVWriter oddWriter = FileOperations.getInstance().openWriter(odd, fs, conf, aconf);
    oddWriter.startDefaultLocalityGroup();
    long ts = System.currentTimeMillis();
    Text cf = new Text("cf");
    Text cq = new Text("cq");
    Value value = new Value("value".getBytes());
    for (int i = 0; i < 100; i += 2) {
      Key key = new Key(new Text(String.format("%8d", i)), cf, cq, ts);
      evenWriter.append(key, value);
      key = new Key(new Text(String.format("%8d", i + 1)), cf, cq, ts);
      oddWriter.append(key, value);
    }
    evenWriter.close();
    oddWriter.close();
    exec("createtable " + table, true);
    exec("importdirectory " + importDir + " " + errorsDir + " true", true);
    exec("scan -r 00000000", true, "00000000", true);
    exec("scan -r 00000099", true, "00000099", true);
    exec("deletetable -f " + table);
  }

  @Test(timeout = 45000)
  public void info() throws Exception {
    exec("info", true, Constants.VERSION, true);
  }

  @Test(timeout = 45000)
  public void interpreter() throws Exception {
    final String table = name.getMethodName();

    exec("createtable " + table, true);
    exec("interpreter -l", true, "HexScan", false);
    exec("insert \\x02 cf cq value", true);
    exec("scan -b 02", true, "value", false);
    exec("interpreter -i org.apache.accumulo.core.util.interpret.HexScanInterpreter", true);

    // Need to allow time for this to propagate through zoocache/zookeeper
    Thread.sleep(3000);

    exec("interpreter -l", true, "HexScan", true);
    exec("scan -b 02", true, "value", true);
    exec("deletetable -f " + table, true);
  }

  @Test(timeout = 45000)
  public void listcompactions() throws Exception {
    final String table = name.getMethodName();

    exec("createtable " + table, true);
    exec("config -t " + table + " -s table.iterator.minc.slow=30,org.apache.accumulo.test.functional.SlowIterator", true);
    exec("config -t " + table + " -s table.iterator.minc.slow.opt.sleepTime=1000", true);
    exec("insert a cf cq value", true);
    exec("insert b cf cq value", true);
    exec("insert c cf cq value", true);
    exec("insert d cf cq value", true);
    exec("flush -t " + table, true);
    exec("sleep 0.2", true);
    exec("listcompactions", true, "default_tablet");
    String[] lines = output.get().split("\n");
    String last = lines[lines.length - 1];
    String[] parts = last.split("\\|");
    assertEquals(12, parts.length);
    exec("deletetable -f " + table, true);
  }

  @Test(timeout = 45000)
  public void maxrow() throws Exception {
    final String table = name.getMethodName();

    exec("createtable " + table, true);
    exec("insert a cf cq value", true);
    exec("insert b cf cq value", true);
    exec("insert ccc cf cq value", true);
    exec("insert zzz cf cq value", true);
    exec("maxrow", true, "zzz", true);
    exec("delete zzz cf cq", true);
    exec("maxrow", true, "ccc", true);
    exec("deletetable -f " + table, true);
  }

  @Test(timeout = 45000)
  public void merge() throws Exception {
    final String table = name.getMethodName();

    exec("createtable " + table);
    exec("addsplits a m z");
    exec("getsplits", true, "z", true);
    exec("merge --all", true);
    exec("getsplits", true, "z", false);
    exec("deletetable -f " + table);
    exec("getsplits -t !METADATA", true);
    assertEquals(3, output.get().split("\n").length);
    exec("merge --all -t !METADATA");
    exec("getsplits -t !METADATA", true);
    assertEquals(2, output.get().split("\n").length);
  }

  @Test(timeout = 45000)
  public void ping() throws Exception {
    for (int i = 0; i < 10; i++) {
      exec("ping", true, "OK", true);
      // wait for both tservers to start up
      if (output.get().split("\n").length == 3)
        break;
      UtilWaitThread.sleep(1000);

    }
    assertEquals(3, output.get().split("\n").length);
  }

  @Test(timeout = 45000)
  public void renametable() throws Exception {
    final String table = name.getMethodName() + "1", rename = name.getMethodName() + "2";

    exec("createtable " + table);
    exec("insert this is a value");
    exec("renametable " + table + " " + rename);
    exec("tables", true, rename, true);
    exec("tables", true, table, false);
    exec("scan -t " + rename, true, "value", true);
    exec("deletetable -f " + rename, true);
  }

  @Test(timeout = 45000)
  public void systempermission() throws Exception {
    exec("systempermissions");
    assertEquals(8, output.get().split("\n").length - 1);
    exec("tablepermissions", true);
    assertEquals(6, output.get().split("\n").length - 1);
  }

  @Test(timeout = 45000)
  public void listscans() throws Exception {
    final String table = name.getMethodName();

    exec("createtable " + table, true);

    // Should be about a 3 second scan
    for (int i = 0; i < 6; i++) {
      exec("insert " + i + " cf cq value", true);
    }

    ZooKeeperInstance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector connector = instance.getConnector("root", new PasswordToken(secret));
    final Scanner s = connector.createScanner(table, Constants.NO_AUTHS);
    IteratorSetting cfg = new IteratorSetting(30, SlowIterator.class);
    cfg.addOption("sleepTime", "500");
    s.addScanIterator(cfg);

    Thread thread = new Thread() {
      public void run() {
        try {
          for (@SuppressWarnings("unused")
          Entry<Key,Value> kv : s)
            ;
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    };
    thread.start();

    List<String> scans = new ArrayList<String>();
    // Try to find the active scan for about 15seconds
    for (int i = 0; i < 50 && scans.isEmpty(); i++) {
      String currentScans = exec("listscans", true);
      String[] lines = currentScans.split("\n");
      for (int scanOffset = 2; scanOffset < lines.length; scanOffset++) {
        String currentScan = lines[scanOffset];
        if (currentScan.contains(table)) {
          scans.add(currentScan);
        }
      }
      UtilWaitThread.sleep(300);
    }
    thread.join();

    assertFalse("Could not find any active scans over table " + table, scans.isEmpty());

    for (String scan : scans) {
      assertTrue("Scan does not appear to be a 'RUNNING' scan: '" + scan + "'", scan.contains("RUNNING"));
      String parts[] = scan.split("\\|");
      assertEquals("Expected 13 colums, but found " + parts.length + " instead for '" + Arrays.toString(parts) + "'", 13, parts.length);
    }

    exec("deletetable -f " + table, true);
  }

  @Test(timeout = 45000)
  public void testPertableClasspath() throws Exception {
    final String table = name.getMethodName();

    File fooFilterJar = File.createTempFile("FooFilter", ".jar");
    FileUtils.copyURLToFile(this.getClass().getResource("/FooFilter.jar"), fooFilterJar);
    fooFilterJar.deleteOnExit();

    File fooConstraintJar = File.createTempFile("FooConstraint", ".jar");
    FileUtils.copyURLToFile(this.getClass().getResource("/FooConstraint.jar"), fooConstraintJar);
    fooConstraintJar.deleteOnExit();

    exec(
        "config -s " + Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1=" + fooFilterJar.toURI().toString() + "," + fooConstraintJar.toURI().toString(),
        true);

    exec("createtable " + table, true);
    exec("config -t " + table + " -s " + Property.TABLE_CLASSPATH.getKey() + "=cx1", true);

    UtilWaitThread.sleep(200);

    // We can't use the setiter command as Filter implements OptionDescriber which
    // forces us to enter more input that I don't know how to input
    // Instead, we can just manually set the property on the table.
    exec("config -t " + table + " -s " + Property.TABLE_ITERATOR_PREFIX.getKey() + "scan.foo=10,org.apache.accumulo.test.FooFilter");

    exec("insert foo f q v", true);

    UtilWaitThread.sleep(100);

    exec("scan -np", true, "foo", false);

    exec("constraint -a FooConstraint", true);

    exec("offline " + table);
    UtilWaitThread.sleep(500);
    exec("online " + table);

    exec("table " + table, true);
    exec("insert foo f q v", false);
    exec("insert ok foo q v", true);

    exec("deletetable -f " + table, true);
    exec("config -d " + Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1");

  }

  @Test(timeout = 45000)
  public void trace() throws Exception {
    // Make sure to not collide with the "trace" table
    final String table = name.getMethodName() + "Test";

    exec("trace on", true);
    exec("createtable " + table, true);
    exec("insert a b c value", true);
    exec("scan -np", true, "value", true);
    exec("deletetable -f " + table);
    exec("sleep 1");
    String trace = exec("trace off");
    assertTrue(trace.contains("binMutations"));
    assertTrue(trace.contains("update"));
    assertTrue(trace.contains("DeleteTable"));
  }

  private int countkeys(String table) throws IOException {
    exec("scan -np -t " + table);
    return output.get().split("\n").length - 1;
  }

  private void make10() throws IOException {
    for (int i = 0; i < 10; i++) {
      exec(String.format("insert row%d cf col%d value", i, i));
    }
  }

  private List<String> getFiles(String tableId) throws IOException {
    output.clear();

    exec("scan -t !METADATA -np -c file -b " + tableId + " -e " + tableId + "~");

    String[] lines = StringUtils.split(output.get(), "\n");
    output.clear();

    if (0 == lines.length) {
      return Collections.emptyList();
    }

    return Arrays.asList(Arrays.copyOfRange(lines, 1, lines.length));
  }

  private int countFiles(String tableId) throws IOException {
    return getFiles(tableId).size();
  }

  private String getTableId(String tableName) throws Exception {
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));

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
