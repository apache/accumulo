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
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.newBufferedReader;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.client.summary.summarizers.DeletesSummarizer;
import org.apache.accumulo.core.client.summary.summarizers.FamilySummarizer;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterConfig;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.accumulo.test.categories.SunnyDayTests;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.tracer.TraceServer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.DistCp;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Category({MiniClusterOnlyTests.class, SunnyDayTests.class})
public class ShellServerIT extends SharedMiniClusterBase {

  @SuppressWarnings("removal")
  private static final Property VFS_CONTEXT_CLASSPATH_PROPERTY =
      Property.VFS_CONTEXT_CLASSPATH_PROPERTY;

  public static class TestOutputStream extends OutputStream {
    StringBuilder sb = new StringBuilder();

    @Override
    public void write(int b) {
      sb.append((char) (0xff & b));
    }

    public String get() {
      return sb.toString();
    }

    public void clear() {
      sb.setLength(0);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(ShellServerIT.class);

  public static class StringInputStream extends InputStream {
    private String source = "";
    private int offset = 0;

    @Override
    public int read() {
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

  private abstract static class ErrorMessageCallback {
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
    private static final Logger shellLog = LoggerFactory.getLogger(TestShell.class);
    public TestOutputStream output;
    public StringInputStream input;
    public Shell shell;
    public LineReader reader;
    public Terminal terminal;

    TestShell(String user, String rootPass, String instanceName, String zookeepers, File configFile)
        throws IOException {
      ClientInfo info = ClientInfo.from(configFile.toPath());
      // start the shell
      output = new TestOutputStream();
      input = new StringInputStream();
      terminal = new DumbTerminal(input, output);
      terminal.setSize(new Size(80, 24));
      reader = LineReaderBuilder.builder().terminal(terminal).build();
      shell = new Shell(reader);
      shell.setLogErrorsToConsole();
      if (info.saslEnabled()) {
        // Pull the kerberos principal out when we're using SASL
        shell.config("-u", user, "-z", instanceName, zookeepers, "--config-file",
            configFile.getAbsolutePath());
      } else {
        shell.config("-u", user, "-p", rootPass, "-z", instanceName, zookeepers, "--config-file",
            configFile.getAbsolutePath());
      }
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

    String exec(String cmd, boolean expectGoodExit, ErrorMessageCallback callback)
        throws IOException {
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

    String exec(String cmd, boolean expectGoodExit, String expectString,
        ErrorMessageCallback callback) throws IOException {
      return exec(cmd, expectGoodExit, expectString, true, callback);
    }

    String exec(String cmd, boolean expectGoodExit, String expectString, boolean stringPresent)
        throws IOException {
      return exec(cmd, expectGoodExit, expectString, stringPresent, noop);
    }

    String exec(String cmd, boolean expectGoodExit, String expectString, boolean stringPresent,
        ErrorMessageCallback callback) throws IOException {
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
      shellLog.debug("Shell Output: '{}'", output.get());
      if (shell.getExitCode() != 0) {
        String errorMsg = callback.getErrorMessage();
        assertEquals(errorMsg, 0, shell.getExitCode());
      }

      if (!s.isEmpty())
        assertEquals(s + " present in " + output.get() + " was not " + stringPresent, stringPresent,
            output.get().contains(s));
    }

    void assertBadExit(String s, boolean stringPresent, ErrorMessageCallback callback) {
      shellLog.debug(output.get());
      if (shell.getExitCode() == 0) {
        String errorMsg = callback.getErrorMessage();
        assertTrue(errorMsg, shell.getExitCode() > 0);
      }

      if (!s.isEmpty())
        assertEquals(s + " present in " + output.get() + " was not " + stringPresent, stringPresent,
            output.get().contains(s));
      shell.resetExitCode();
    }
  }

  private static final NoOpErrorMessageCallback noop = new NoOpErrorMessageCallback();

  private TestShell ts;

  private static Process traceProcess;
  private static String rootPath;

  @Rule
  public TestName name = new TestName();

  private static class ShellServerITConfigCallback implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      // Only one tserver to avoid race conditions on ZK propagation (auths and configuration)
      cfg.setNumTservers(1);
      // Set the min span to 0 so we will definitely get all the traces back. See ACCUMULO-4365
      Map<String,String> siteConf = cfg.getSiteConfig();
      siteConf.put(Property.TRACE_SPAN_RECEIVER_PREFIX.getKey() + "tracer.span.min.ms", "0");
      cfg.setSiteConfig(siteConf);
    }
  }

  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new ShellServerITConfigCallback());
    rootPath = getMiniClusterDir().getAbsolutePath();

    String userDir = System.getProperty("user.dir");

    // history file is updated in $HOME
    System.setProperty("HOME", rootPath);
    System.setProperty("hadoop.tmp.dir", userDir + "/target/hadoop-tmp");

    traceProcess = getCluster().exec(TraceServer.class).getProcess();

    Properties props = getCluster().getClientProperties();
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      TableOperations tops = client.tableOperations();
      // give the tracer some time to start
      while (!tops.exists("trace")) {
        sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
    }
  }

  @Before
  public void setupShell() throws Exception {
    ts = new TestShell(getPrincipal(), getRootPassword(),
        getCluster().getConfig().getInstanceName(), getCluster().getConfig().getZooKeepers(),
        getCluster().getConfig().getClientPropsFile());
  }

  @AfterClass
  public static void tearDownAfterClass() {
    if (traceProcess != null) {
      traceProcess.destroy();
    }

    SharedMiniClusterBase.stopMiniCluster();
  }

  @After
  public void deleteTables() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      for (String table : c.tableOperations().list()) {
        if (!table.startsWith(Namespace.ACCUMULO.name() + ".") && !table.equals("trace"))
          try {
            c.tableOperations().delete(table);
          } catch (TableNotFoundException e) {
            // don't care
          }
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

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @Test
  public void exporttableImporttable() throws Exception {
    final String table = name.getMethodName(), table2 = table + "2";

    // exporttable / importtable
    ts.exec("createtable " + table + " -evc", true);
    make10();
    ts.exec("addsplits row5", true);
    ts.exec("config -t " + table + " -s table.split.threshold=345M", true);
    ts.exec("offline " + table, true);
    File exportDir = new File(rootPath, "ShellServerIT.export");
    String exportUri = "file://" + exportDir;
    String localTmp = "file://" + new File(rootPath, "ShellServerIT.tmp");
    ts.exec("exporttable -t " + table + " " + exportUri, true);
    DistCp cp = newDistCp(new Configuration(false));
    String import_ = "file://" + new File(rootPath, "ShellServerIT.import");
    ClientInfo info = ClientInfo.from(getCluster().getClientProperties());
    if (info.saslEnabled()) {
      // DistCp bugs out trying to get a fs delegation token to perform the cp. Just copy it
      // ourselves by hand.
      FileSystem fs = getCluster().getFileSystem();
      FileSystem localFs = FileSystem.getLocal(new Configuration(false));

      // Path on local fs to cp into
      Path localTmpPath = new Path(localTmp);
      localFs.mkdirs(localTmpPath);

      // Path in remote fs to importtable from
      Path importDir = new Path(import_);
      fs.mkdirs(importDir);

      // Implement a poor-man's DistCp
      try (BufferedReader reader =
          new BufferedReader(new FileReader(new File(exportDir, "distcp.txt"), UTF_8))) {
        for (String line; (line = reader.readLine()) != null;) {
          Path exportedFile = new Path(line);
          // There isn't a cp on FileSystem??
          log.info("Copying {} to {}", line, localTmpPath);
          fs.copyToLocalFile(exportedFile, localTmpPath);
          Path tmpFile = new Path(localTmpPath, exportedFile.getName());
          log.info("Moving {} to the import directory {}", tmpFile, importDir);
          fs.moveFromLocalFile(tmpFile, importDir);
        }
      }
    } else {
      String[] distCpArgs = {"-f", exportUri + "/distcp.txt", import_};
      assertEquals("Failed to run distcp: " + Arrays.toString(distCpArgs), 0, cp.run(distCpArgs));
    }
    ts.exec("importtable " + table2 + " " + import_, true);
    ts.exec("config -t " + table2 + " -np", true, "345M", true);
    ts.exec("getsplits -t " + table2, true, "row5", true);
    ts.exec("constraint --list -t " + table2, true, "VisibilityConstraint=2", true);
    ts.exec("online " + table, true);
    ts.exec("deletetable -f " + table, true);
    ts.exec("deletetable -f " + table2, true);
  }

  private DistCp newDistCp(Configuration conf) {
    try {
      @SuppressWarnings("unchecked")
      Constructor<DistCp>[] constructors = (Constructor<DistCp>[]) DistCp.class.getConstructors();
      for (Constructor<DistCp> constructor : constructors) {
        Class<?>[] parameterTypes = constructor.getParameterTypes();
        if (parameterTypes.length > 0 && parameterTypes[0].equals(Configuration.class)) {
          if (parameterTypes.length == 1) {
            return constructor.newInstance(conf);
          } else if (parameterTypes.length == 2) {
            return constructor.newInstance(conf, null);
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
    ts.input.set("true\n\n\n\nSTRING");
    ts.exec("setscaniter -class " + SUMMING_COMBINER_ITERATOR + " -p 10 -n name", true);
    ts.exec("scan", true, "3", true);
    ts.exec("deletescaniter -n name", true);
    ts.exec("scan", true, "1", true);
    ts.exec("deletetable -f " + table);

  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
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
    assertEquals(3, lines.split("\n").length - 1);
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
    assertTrue("Output did not match regex: '" + o + "'",
        o.matches(".*[1-9][0-9][0-9]\\s\\[" + table + "\\]\\n"));
    ts.exec("deletetable -f " + table);
  }

  /*
   * This test should be deleted when the debug command is removed
   */
  @Deprecated(since = "2.0.0")
  @Test
  public void debug() throws Exception {
    String expectMsg = "The debug command is deprecated";
    ts.exec("debug", false, expectMsg);
    ts.exec("debug on", false, expectMsg);
    ts.exec("debug", false, expectMsg);
    ts.exec("debug off", false, expectMsg);
    ts.exec("debug", false, expectMsg);
    ts.exec("debug debug", false, expectMsg);
    ts.exec("debug debug debug", false, expectMsg);
  }

  @Test
  public void user() throws Exception {
    final String table = name.getMethodName();
    final boolean kerberosEnabled = getToken() instanceof KerberosToken;

    // createuser, deleteuser, user, users, droptable, grant, revoke
    if (!kerberosEnabled) {
      ts.input.set("secret\nsecret\n");
    }
    ts.exec("createuser xyzzy", true);
    ts.exec("users", true, "xyzzy", true);
    String perms = ts.exec("userpermissions -u xyzzy", true);
    assertTrue(perms.contains("Table permissions (" + MetadataTable.NAME + "): Table.READ"));
    ts.exec("grant -u xyzzy -s System.CREATE_TABLE", true);
    perms = ts.exec("userpermissions -u xyzzy", true);
    assertTrue(perms.contains(""));
    ts.exec("grant -u " + getPrincipal() + " -t " + MetadataTable.NAME + " Table.WRITE", true);
    ts.exec("grant -u " + getPrincipal() + " -t " + MetadataTable.NAME + " Table.GOOFY", false);
    ts.exec("grant -u " + getPrincipal() + " -s foo", false);
    ts.exec("grant -u xyzzy -t " + MetadataTable.NAME + " foo", false);
    if (!kerberosEnabled) {
      ts.input.set("secret\nsecret\n");
      ts.exec("user xyzzy", true);
      ts.exec("createtable " + table, true, "xyzzy@", true);
      ts.exec("insert row1 cf cq 1", true);
      ts.exec("scan", true, "row1", true);
      ts.exec("droptable -f " + table, true);
      ts.input.set(getRootPassword() + "\n" + getRootPassword() + "\n");
      ts.exec("user root", true);
    }
    ts.exec("deleteuser " + getPrincipal(), false, "delete yourself", true);
    ts.exec("revoke -u xyzzy -s System.CREATE_TABLE", true);
    ts.exec("revoke -u xyzzy -s System.GOOFY", false);
    ts.exec("revoke -u xyzzy -s foo", false);
    ts.exec("revoke -u xyzzy -t " + MetadataTable.NAME + " Table.WRITE", true);
    ts.exec("revoke -u xyzzy -t " + MetadataTable.NAME + " Table.GOOFY", false);
    ts.exec("revoke -u xyzzy -t " + MetadataTable.NAME + " foo", false);
    ts.exec("deleteuser xyzzy", true, "deleteuser { xyzzy } (yes|no)?", true);
    ts.exec("deleteuser -f xyzzy", true);
    ts.exec("users", true, "xyzzy", false);
  }

  @Test
  public void durability() throws Exception {
    final String table = name.getMethodName();
    ts.exec("createtable " + table);
    ts.exec("insert -d none a cf cq randomGunkaASDFWEAQRd");
    ts.exec("insert -d foo a cf cq2 2", false, "foo", true);
    ts.exec("scan -r a", true, "randomGunkaASDFWEAQRd", true);
    ts.exec("scan -r a", true, "foo", false);
  }

  @Test
  public void iter() throws Exception {
    final String table = name.getMethodName();

    // setshelliter, listshelliter, deleteshelliter
    ts.exec("createtable " + table);
    ts.exec("insert a cf cq 1");
    ts.exec("insert a cf cq 1");
    ts.exec("insert a cf cq 1");
    ts.input.set("true\n\n\n\nSTRING\n");
    ts.exec("setshelliter -class " + SUMMING_COMBINER_ITERATOR + " -p 10 -pn sum -n name", true);
    ts.exec("setshelliter -class " + SUMMING_COMBINER_ITERATOR + " -p 11 -pn sum -n name", false);
    ts.exec("setshelliter -class " + SUMMING_COMBINER_ITERATOR + " -p 10 -pn sum -n other", false);
    ts.input.set("true\n\n\n\nSTRING\n");
    ts.exec("setshelliter -class " + SUMMING_COMBINER_ITERATOR + " -p 11 -pn sum -n xyzzy", true);
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
    ts.input.set("true\n\n\n\nSTRING\n");
    ts.exec("setiter -scan -class " + SUMMING_COMBINER_ITERATOR + " -p 10 -n name", true);
    ts.exec("setiter -scan -class " + SUMMING_COMBINER_ITERATOR + " -p 11 -n name", false);
    ts.exec("setiter -scan -class " + SUMMING_COMBINER_ITERATOR + " -p 10 -n other", false);
    ts.input.set("true\n\n\n\nSTRING\n");
    ts.exec("setiter -scan -class " + SUMMING_COMBINER_ITERATOR + " -p 11 -n xyzzy", true);
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
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = name.getMethodName();

      ts.exec("createtable " + tableName);
      ts.input.set("\n\n");
      // Setting a non-optiondescriber with no name should fail
      ts.exec("setiter -scan -class " + COLUMN_FAMILY_COUNTER_ITERATOR + " -p 30", false);

      // Name as option will work
      ts.exec("setiter -scan -class " + COLUMN_FAMILY_COUNTER_ITERATOR + " -p 30 -name cfcounter",
          true);

      String expectedKey = "table.iterator.scan.cfcounter";
      String expectedValue = "30," + COLUMN_FAMILY_COUNTER_ITERATOR;
      TableOperations tops = client.tableOperations();
      checkTableForProperty(tops, tableName, expectedKey, expectedValue);

      ts.exec("deletetable " + tableName, true);
      tableName = tableName + "1";

      ts.exec("createtable " + tableName, true);

      ts.input.set("customcfcounter\n\n");

      // Name on the CLI should override OptionDescriber (or user input name, in this case)
      ts.exec("setiter -scan -class " + COLUMN_FAMILY_COUNTER_ITERATOR + " -p 30", true);
      expectedKey = "table.iterator.scan.customcfcounter";
      expectedValue = "30," + COLUMN_FAMILY_COUNTER_ITERATOR;
      checkTableForProperty(tops, tableName, expectedKey, expectedValue);

      ts.exec("deletetable " + tableName, true);
      tableName = tableName + "1";

      ts.exec("createtable " + tableName, true);

      ts.input.set("customcfcounter\nname1 value1\nname2 value2\n\n");

      // Name on the CLI should override OptionDescriber (or user input name, in this case)
      ts.exec("setiter -scan -class " + COLUMN_FAMILY_COUNTER_ITERATOR + " -p 30", true);
      expectedKey = "table.iterator.scan.customcfcounter";
      expectedValue = "30," + COLUMN_FAMILY_COUNTER_ITERATOR;
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
      ts.exec("setiter -scan -class " + COLUMN_FAMILY_COUNTER_ITERATOR + " -p 30 -name cfcounter",
          true);
      expectedKey = "table.iterator.scan.cfcounter";
      expectedValue = "30," + COLUMN_FAMILY_COUNTER_ITERATOR;
      checkTableForProperty(tops, tableName, expectedKey, expectedValue);
      expectedKey = "table.iterator.scan.cfcounter.opt.name1";
      expectedValue = "value1.1,value1.2,value1.3";
      checkTableForProperty(tops, tableName, expectedKey, expectedValue);
      expectedKey = "table.iterator.scan.cfcounter.opt.name2";
      expectedValue = "value2";
      checkTableForProperty(tops, tableName, expectedKey, expectedValue);
    }
  }

  protected void checkTableForProperty(TableOperations tops, String tableName, String expectedKey,
      String expectedValue) throws Exception {
    for (int i = 0; i < 5; i++) {
      for (Entry<String,String> entry : tops.getProperties(tableName)) {
        if (expectedKey.equals(entry.getKey())) {
          assertEquals(expectedValue, entry.getValue());
          return;
        }
      }
      Thread.sleep(500);
    }

    fail("Failed to find expected property on " + tableName + ": " + expectedKey + "="
        + expectedValue);
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
    // Rely on the timeout rule in AccumuloIT
    while (!success) {
      try {
        ts.exec("insert a b c d -l foo", false, "does not have authorization", true,
            new ErrorMessageCallback() {
              @Override
              public String getErrorMessage() {
                try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
                  return "Current auths for root are: "
                      + c.securityOperations().getUserAuthorizations("root");
                } catch (Exception e) {
                  return "Could not check authorizations";
                }
              }
            });
        success = true;
      } catch (AssertionError e) {
        Thread.sleep(500);
      }
    }
    ts.exec("addauths -s foo,bar", true);
    boolean passed = false;
    // Rely on the timeout rule in AccumuloIT
    while (!passed) {
      try {
        ts.exec("getauths", true, "foo", true);
        ts.exec("getauths", true, "bar", true);
        passed = true;
      } catch (AssertionError | Exception e) {
        sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
      }
    }
    assertTrue("Could not successfully see updated authoriations", passed);
    ts.exec("insert a b c d -l foo");
    ts.exec("scan", true, "[foo]");
    ts.exec("scan -s bar", true, "[foo]", false);
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void getAuths() throws Exception {
    Assume.assumeFalse("test skipped for kerberos", getToken() instanceof KerberosToken);

    // create two users with different auths
    for (int i = 1; i <= 2; i++) {
      String userName = name.getMethodName() + "user" + i;
      String password = "password" + i;
      String auths = "auth" + i + "A,auth" + i + "B";
      ts.exec("createuser " + userName, true);
      ts.exec(password, true);
      ts.exec("addauths -u " + userName + " -s " + auths, true);
    }

    // get auths using root user, which has System.SYSTEM
    ts.exec("getauths -u getAuthsuser1", true, "auth1A", true);
    ts.exec("getauths -u getAuthsuser1", true, "auth1B", true);
    ts.exec("getauths -u getAuthsuser2", true, "auth2A", true);
    ts.exec("getauths -u getAuthsuser2", true, "auth2B", true);

    // grant the first user the ability to see other users auths
    ts.exec("grant -u getAuthsuser1 -s System.ALTER_USER", true);

    // switch to first user (the one with the ALTER_USER perm)
    ts.exec("user getAuthsuser1", true);
    ts.exec("password1", true);

    // get auths for self and other user
    ts.exec("getauths -u getAuthsuser1", true, "auth1A", true);
    ts.exec("getauths -u getAuthsuser1", true, "auth1B", true);
    ts.exec("getauths -u getAuthsuser2", true, "auth2A", true);
    ts.exec("getauths -u getAuthsuser2", true, "auth2B", true);

    // switch to second user (the one without the ALTER_USER perm)
    ts.exec("user getAuthsuser2", true);
    ts.exec("password2", true);

    // get auths for self, but not other user
    ts.exec("getauths -u getAuthsuser2", true, "auth2A", true);
    ts.exec("getauths -u getAuthsuser2", true, "auth2B", true);
    ts.exec("getauths -u getAuthsuser1", false, "PERMISSION_DENIED", true);
    ts.exec("getauths -u getAuthsuser1", false, "PERMISSION_DENIED", true);
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
    ts.exec("classpath", true,
        "Level: 2, Name: app, class: jdk.internal.loader.ClassLoaders$AppClassLoader: configuration not inspectable",
        true);
  }

  @Test
  public void clearCls() throws Exception {
    // clear/cls
    if (!Terminal.TYPE_DUMB.equalsIgnoreCase(ts.shell.getTerminal().getType())) {
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
    ts.exec("table " + clone);
    ts.exec("scan", true, "value", true);
    ts.exec("constraint --list -t " + clone, true, "VisibilityConstraint=2", true);
    ts.exec("config -t " + clone + " -np", true, "123M", true);
    ts.exec("getsplits -t " + clone, true, "a\nb\nc\n");
    ts.exec("deletetable -f " + table);
    ts.exec("deletetable -f " + clone);
  }

  @Test
  public void clonetableOffline() throws Exception {
    final String table = name.getMethodName(), clone = table + "_clone";

    // clonetable
    ts.exec("createtable " + table + " -evc");
    ts.exec("config -t " + table + " -s table.split.threshold=123M", true);
    ts.exec("addsplits -t " + table + " a b c", true);
    ts.exec("insert a b c value");
    ts.exec("scan", true, "value", true);
    ts.exec("clonetable " + table + " " + clone + " -o");
    // verify constraint, config, and splits were cloned
    ts.exec("table " + clone);
    ts.exec("scan", false, "TableOfflineException", true);
    ts.exec("constraint --list -t " + clone, true, "VisibilityConstraint=2", true);
    ts.exec("config -t " + clone + " -np", true, "123M", true);
    ts.exec("getsplits -t " + clone, true, "a\nb\nc\n");
    ts.exec("deletetable -f " + table);
    ts.exec("deletetable -f " + clone);
  }

  @Test
  public void createTableWithProperties() throws Exception {
    final String table = name.getMethodName();

    // create table with initial properties
    String testProp = "table.custom.description=description,table.custom.testProp=testProp,"
        + Property.TABLE_SPLIT_THRESHOLD.getKey() + "=10K";

    ts.exec("createtable " + table + " -prop " + testProp, true);
    ts.exec("insert a b c value", true);
    ts.exec("scan", true, "value", true);

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      for (Entry<String,String> entry : accumuloClient.tableOperations().getProperties(table)) {
        if (entry.getKey().equals("table.custom.description"))
          assertEquals("Initial property was not set correctly", "description", entry.getValue());

        if (entry.getKey().equals("table.custom.testProp"))
          assertEquals("Initial property was not set correctly", "testProp", entry.getValue());

        if (entry.getKey().equals(Property.TABLE_SPLIT_THRESHOLD.getKey()))
          assertEquals("Initial property was not set correctly", "10K", entry.getValue());

      }
    }
    ts.exec("deletetable -f " + table);
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
    ts.exec("insert n 1 2 v901");
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

    // test compaction strategy
    ts.exec("insert z 1 2 v900");
    ts.exec("compact -w -s " + TestCompactionStrategy.class.getName()
        + " -sc inputPrefix=F,dropPrefix=A");
    assertEquals(1, countFiles(tableId));
    ts.exec("scan", true, "v900", true);
    ts.exec("scan", true, "v901", false);

    ts.exec("deletetable -f " + table);
  }

  @Test
  public void testCompactionSelection() throws Exception {
    final String table = name.getMethodName();
    final String clone = table + "_clone";

    ts.exec("createtable " + table);
    ts.exec("insert a b c d");
    ts.exec("flush -w");
    ts.exec("insert x y z v");
    ts.exec("flush -w");

    ts.exec("clonetable -s " + Property.TABLE_MAJC_RATIO.getKey() + "=10 " + table + " " + clone);

    ts.exec("table " + clone);
    ts.exec("insert m n l o");
    ts.exec("flush -w");

    String tableId = getTableId(table);
    String cloneId = getTableId(clone);

    assertEquals(3, countFiles(cloneId));

    // compact only files from src table
    ts.exec("compact -t " + clone + " -w --sf-epath .*tables/" + tableId + ".*");

    assertEquals(2, countFiles(cloneId));

    ts.exec("insert r s t u");
    ts.exec("flush -w");

    assertEquals(3, countFiles(cloneId));

    // compact all flush files
    ts.exec("compact -t " + clone + " -w --sf-ename F.*");

    assertEquals(2, countFiles(cloneId));

    // create two large files
    Random rand = new SecureRandom();
    StringBuilder sb = new StringBuilder("insert b v q ");
    for (int i = 0; i < 10000; i++) {
      sb.append('a' + rand.nextInt(26));
    }

    ts.exec(sb.toString());
    ts.exec("flush -w");

    ts.exec(sb.toString());
    ts.exec("flush -w");

    assertEquals(4, countFiles(cloneId));

    // compact only small files
    ts.exec("compact -t " + clone + " -w --sf-lt-esize 1000");

    assertEquals(3, countFiles(cloneId));

    // compact large files if 3 or more
    ts.exec("compact -t " + clone + " -w --sf-gt-esize 1K --min-files 3");

    assertEquals(3, countFiles(cloneId));

    // compact large files if 2 or more
    ts.exec("compact -t " + clone + " -w --sf-gt-esize 1K --min-files 2");

    assertEquals(2, countFiles(cloneId));

    // compact if tablet has 3 or more files
    ts.exec("compact -t " + clone + " -w --min-files 3");

    assertEquals(2, countFiles(cloneId));

    // compact if tablet has 2 or more files
    ts.exec("compact -t " + clone + " -w --min-files 2");

    assertEquals(1, countFiles(cloneId));

    // create two small and one large flush files in order to test AND
    ts.exec(sb.toString());
    ts.exec("flush -w");

    ts.exec("insert m n l o");
    ts.exec("flush -w");

    ts.exec("insert m n l o");
    ts.exec("flush -w");

    assertEquals(4, countFiles(cloneId));

    // should only compact two small flush files leaving large flush file
    ts.exec("compact -t " + clone + " -w --sf-ename F.* --sf-lt-esize 1K");

    assertEquals(3, countFiles(cloneId));

    String clone2 = table + "_clone_2";
    ts.exec("clonetable -s"
        + " table.sampler.opt.hasher=murmur3_32,table.sampler.opt.modulus=7,table.sampler="
        + RowSampler.class.getName() + " " + clone + " " + clone2);
    String clone2Id = getTableId(clone2);

    assertEquals(3, countFiles(clone2Id));

    ts.exec("table " + clone2);
    ts.exec("insert v n l o");
    ts.exec("flush -w");

    ts.exec("insert x n l o");
    ts.exec("flush -w");

    assertEquals(5, countFiles(clone2Id));

    ts.exec("compact -t " + clone2 + " -w --sf-no-sample");

    assertEquals(3, countFiles(clone2Id));
  }

  @Test
  public void testCompactionSelectionAndStrategy() throws Exception {

    final String table = name.getMethodName();

    ts.exec("createtable " + table);

    // expect this to fail
    ts.exec("compact -t " + table + " -w --sf-ename F.* -s "
        + TestCompactionStrategy.class.getName() + " -sc inputPrefix=F,dropPrefix=A", false);
  }

  @Test
  public void testScanScample() throws Exception {
    final String table = name.getMethodName();

    // compact
    ts.exec("createtable " + table);

    ts.exec("insert 9255 doc content 'abcde'");
    ts.exec("insert 9255 doc url file://foo.txt");
    ts.exec("insert 8934 doc content 'accumulo scales'");
    ts.exec("insert 8934 doc url file://accumulo_notes.txt");
    ts.exec("insert 2317 doc content 'milk, eggs, bread, parmigiano-reggiano'");
    ts.exec("insert 2317 doc url file://groceries/9.txt");
    ts.exec("insert 3900 doc content 'EC2 ate my homework'");
    ts.exec("insert 3900 doc uril file://final_project.txt");

    String clone1 = table + "_clone_1";
    ts.exec("clonetable -s"
        + " table.sampler.opt.hasher=murmur3_32,table.sampler.opt.modulus=3,table.sampler="
        + RowSampler.class.getName() + " " + table + " " + clone1);

    ts.exec("compact -t " + clone1 + " -w --sf-no-sample");

    ts.exec("table " + clone1);
    ts.exec("scan --sample", true, "parmigiano-reggiano", true);
    ts.exec("grep --sample reg", true, "parmigiano-reggiano", true);
    ts.exec("scan --sample", true, "accumulo", false);
    ts.exec("grep --sample acc", true, "accumulo", false);

    // create table where table sample config differs from whats in file
    String clone2 = table + "_clone_2";
    ts.exec("clonetable -s"
        + " table.sampler.opt.hasher=murmur3_32,table.sampler.opt.modulus=2,table.sampler="
        + RowSampler.class.getName() + " " + clone1 + " " + clone2);

    ts.exec("table " + clone2);
    ts.exec("scan --sample", false, "SampleNotPresentException", true);
    ts.exec("grep --sample reg", false, "SampleNotPresentException", true);

    ts.exec("compact -t " + clone2 + " -w --sf-no-sample");

    for (String expected : Arrays.asList("2317", "3900", "9255")) {
      ts.exec("scan --sample", true, expected, true);
      ts.exec("grep --sample " + expected.substring(0, 2), true, expected, true);
    }

    ts.exec("scan --sample", true, "8934", false);
    ts.exec("grep --sample 89", true, "8934", false);
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
    sleepUninterruptibly(1, TimeUnit.SECONDS);
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

    log.info("Adding 2 splits");
    ts.exec("addsplits row5 row7");

    log.info("Writing 10 records");
    make10();

    log.info("Flushing table");
    ts.exec("flush -w -t " + table);
    log.info("Table flush completed");

    // One of the tablets we're writing to might migrate inbetween writing data which would create a
    // 2nd file for that tablet
    // If we notice this, compact and then move on.
    List<String> files = getFiles(tableId);
    if (files.size() > 3) {
      log.info("More than 3 files were found, compacting before proceeding");
      ts.exec("compact -w -t " + table);
      files = getFiles(tableId);
      assertEquals("Expected to only find 3 files after compaction: " + files, 3, files.size());
    }

    assertNotNull(files);
    assertEquals("Found the following files: " + files, 3, files.size());
    ts.exec("deleterows -t " + table + " -b row5 -e row7");
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
  public void formatter() throws Exception {
    ts.exec("createtable formatter_test", true);
    ts.exec("table formatter_test", true);
    ts.exec("insert row cf cq 1234abcd", true);
    ts.exec("insert row cf1 cq1 9876fedc", true);
    ts.exec("insert row2 cf cq 13579bdf", true);
    ts.exec("insert row2 cf1 cq 2468ace", true);

    ArrayList<String> expectedDefault = new ArrayList<>(4);
    expectedDefault.add("row cf:cq []\t1234abcd");
    expectedDefault.add("row cf1:cq1 []\t9876fedc");
    expectedDefault.add("row2 cf:cq []\t13579bdf");
    expectedDefault.add("row2 cf1:cq []\t2468ace");
    ArrayList<String> actualDefault = new ArrayList<>(4);
    boolean isFirst = true;
    for (String s : ts.exec("scan -np", true).split("[\n\r]+")) {
      if (isFirst) {
        isFirst = false;
      } else {
        actualDefault.add(s);
      }
    }

    ArrayList<String> expectedFormatted = new ArrayList<>(4);
    expectedFormatted.add("row cf:cq []\t0x31 0x32 0x33 0x34 0x61 0x62 0x63 0x64");
    expectedFormatted.add("row cf1:cq1 []\t0x39 0x38 0x37 0x36 0x66 0x65 0x64 0x63");
    expectedFormatted.add("row2 cf:cq []\t0x31 0x33 0x35 0x37 0x39 0x62 0x64 0x66");
    expectedFormatted.add("row2 cf1:cq []\t0x32 0x34 0x36 0x38 0x61 0x63 0x65");
    ts.exec("formatter -t formatter_test -f " + HexFormatter.class.getName(), true);
    ArrayList<String> actualFormatted = new ArrayList<>(4);
    isFirst = true;
    for (String s : ts.exec("scan -np", true).split("[\n\r]+")) {
      if (isFirst) {
        isFirst = false;
      } else {
        actualFormatted.add(s);
      }
    }

    ts.exec("deletetable -f formatter_test", true);

    assertTrue(Iterables.elementsEqual(expectedDefault, new ArrayList<>(actualDefault)));
    assertTrue(Iterables.elementsEqual(expectedFormatted, new ArrayList<>(actualFormatted)));
  }

  /**
   * Simple <code>Formatter</code> that will convert each character in the Value from decimal to
   * hexadecimal. Will automatically skip over characters in the value which do not fall within the
   * [0-9,a-f] range.
   *
   * <p>
   * Example: <code>'0'</code> will be displayed as <code>'0x30'</code>
   */
  public static class HexFormatter implements Formatter {
    private Iterator<Entry<Key,Value>> iter = null;
    private FormatterConfig config;

    private static final String tab = "\t";
    private static final String newline = "\n";

    @Override
    public boolean hasNext() {
      return this.iter.hasNext();
    }

    @Override
    public String next() {
      final Entry<Key,Value> entry = iter.next();

      String key;

      // Observe the timestamps
      if (config.willPrintTimestamps()) {
        key = entry.getKey().toString();
      } else {
        key = entry.getKey().toStringNoTime();
      }

      final Value v = entry.getValue();

      // Approximate how much space we'll need
      final StringBuilder sb = new StringBuilder(key.length() + v.getSize() * 5);

      sb.append(key).append(tab);

      for (byte b : v.get()) {
        if ((b >= 48 && b <= 57) || (b >= 97 && b <= 102)) {
          sb.append(String.format("0x%x ", (int) b));
        }
      }

      return sb.toString().trim() + newline;
    }

    @Override
    public void remove() {}

    @Override
    public void initialize(final Iterable<Entry<Key,Value>> scanner, final FormatterConfig config) {
      this.iter = scanner.iterator();
      this.config = new FormatterConfig(config);
    }
  }

  @Test
  public void extensions() throws Exception {
    String extName = "ExampleShellExtension";

    // check for example extension
    ts.exec("help", true, extName, false);
    ts.exec("extensions -l", true, extName, false);

    // enable extensions and check for example
    ts.exec("extensions -e", true);
    ts.exec("extensions -l", true, extName, true);
    ts.exec("help", true, extName, true);

    // test example extension command
    ts.exec(extName + "::debug", true, "This is a test", true);

    // disable extensions and check for example
    ts.exec("extensions -d", true);
    ts.exec("extensions -l", true, extName, false);
    ts.exec("help", true, extName, false);

    // ensure extensions are really disabled
    ts.exec(extName + "::debug", true, "Unknown command", true);
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
    for (String c : ("bye exit quit " + "about help info ? "
        + "deleteiter deletescaniter listiter setiter setscaniter "
        + "grant revoke systempermissions tablepermissions userpermissions " + "execfile history "
        + "authenticate cls clear notable sleep table user whoami "
        + "clonetable config createtable deletetable droptable du exporttable "
        + "importtable offline online renametable tables "
        + "addsplits compact constraint flush getgropus getsplits merge setgroups "
        + "addauths createuser deleteuser dropuser getauths passwd setauths users "
        + "delete deletemany deleterows egrep formatter interpreter grep "
        + "importdirectory insert maxrow scan").split(" ")) {
      ts.exec("help " + c, true);
    }
  }

  @Test
  public void history() throws Exception {
    final String table = name.getMethodName();

    ts.exec("history -c", true);
    ts.exec("createtable " + table);
    ts.exec("deletetable -f " + table);
    ts.exec("history", true, table, true);
    ts.exec("history", true, "history", true);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @Test
  public void importDirectoryOld() throws Exception {
    final String table = name.getMethodName();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    File errorsDir = new File(rootPath, "errors_" + table);
    assertTrue(errorsDir.mkdir());
    fs.mkdirs(new Path(errorsDir.toString()));
    File importDir = createRFiles(conf, fs, table);
    ts.exec("createtable " + table, true);
    ts.exec("importdirectory " + importDir + " " + errorsDir + " true", true);
    ts.exec("scan -r 00000000", true, "00000000", true);
    ts.exec("scan -r 00000099", true, "00000099", true);
    ts.exec("deletetable -f " + table);
  }

  @Test
  public void importDirectory() throws Exception {
    final String table = name.getMethodName();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    File importDir = createRFiles(conf, fs, table);
    ts.exec("createtable " + table, true);
    ts.exec("importdirectory " + importDir + " true", true);
    ts.exec("scan -r 00000000", true, "00000000", true);
    ts.exec("scan -r 00000099", true, "00000099", true);
    ts.exec("deletetable -f " + table);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @Test
  public void importDirectoryWithOptions() throws Exception {
    final String table = name.getMethodName();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    File importDir = createRFiles(conf, fs, table);
    ts.exec("createtable " + table, true);
    ts.exec("notable", true);
    ts.exec("importdirectory -t " + table + " -i " + importDir + " true", true);
    ts.exec("scan -t " + table + " -r 00000000", true, "00000000", true);
    ts.exec("scan -t " + table + " -r 00000099", true, "00000099", true);
    // Attempt to re-import without -i option, error should occur
    ts.exec("importdirectory -t " + table + " " + importDir + " true", false);
    // Attempt re-import once more, this time with -i option. No error should occur, only a
    // message indicating the directory was empty and zero files were imported
    ts.exec("importdirectory -t " + table + " -i " + importDir + " true", true);
    ts.exec("scan -t " + table + " -r 00000000", true, "00000000", true);
    ts.exec("scan -t " + table + " -r 00000099", true, "00000099", true);
    ts.exec("deletetable -f " + table);
  }

  private File createRFiles(final Configuration conf, final FileSystem fs, final String postfix)
      throws IOException {
    File importDir = new File(rootPath, "import_" + postfix);
    assertTrue(importDir.mkdir());
    String even = new File(importDir, "even.rf").toString();
    String odd = new File(importDir, "odd.rf").toString();
    AccumuloConfiguration aconf = DefaultConfiguration.getInstance();
    FileSKVWriter evenWriter = FileOperations.getInstance().newWriterBuilder()
        .forFile(even, fs, conf, CryptoServiceFactory.newDefaultInstance())
        .withTableConfiguration(aconf).build();
    evenWriter.startDefaultLocalityGroup();
    FileSKVWriter oddWriter = FileOperations.getInstance().newWriterBuilder()
        .forFile(odd, fs, conf, CryptoServiceFactory.newDefaultInstance())
        .withTableConfiguration(aconf).build();
    oddWriter.startDefaultLocalityGroup();
    long timestamp = System.currentTimeMillis();
    Text cf = new Text("cf");
    Text cq = new Text("cq");
    Value value = new Value("value");
    for (int i = 0; i < 100; i += 2) {
      Key key = new Key(new Text(String.format("%8d", i)), cf, cq, timestamp);
      evenWriter.append(key, value);
      key = new Key(new Text(String.format("%8d", i + 1)), cf, cq, timestamp);
      oddWriter.append(key, value);
    }
    evenWriter.close();
    oddWriter.close();
    assertEquals(0, ts.shell.getExitCode());
    return importDir;
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
    sleepUninterruptibly(3, TimeUnit.SECONDS);

    ts.exec("interpreter -l", true, "HexScan", true);
    ts.exec("scan -b 02", true, "value", true);
    ts.exec("deletetable -f " + table, true);
  }

  @Test
  public void listcompactions() throws Exception {
    final String table = name.getMethodName();

    ts.exec("createtable " + table, true);
    ts.exec(
        "config -t " + table
            + " -s table.iterator.minc.slow=30,org.apache.accumulo.test.functional.SlowIterator",
        true);
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
      sleepUninterruptibly(1, TimeUnit.SECONDS);

    }
    assertEquals(2, ts.output.get().split("\n").length);
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
    assertEquals(12, ts.output.get().split("\n").length - 1);
    ts.exec("tablepermissions", true);
    assertEquals(7, ts.output.get().split("\n").length - 1);
  }

  @Test
  public void listscans() throws Exception {
    final String table = name.getMethodName();

    ts.exec("createtable " + table, true);

    // Should be about a 3 second scan
    for (int i = 0; i < 6; i++) {
      ts.exec("insert " + i + " cf cq value", true);
    }

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build();
        Scanner s = accumuloClient.createScanner(table, Authorizations.EMPTY)) {
      IteratorSetting cfg = new IteratorSetting(30, SlowIterator.class);
      SlowIterator.setSleepTime(cfg, 500);
      s.addScanIterator(cfg);

      Thread thread = new Thread(() -> Iterators.size(s.iterator()));
      thread.start();

      List<String> scans = new ArrayList<>();
      // Try to find the active scan for about 15seconds
      for (int i = 0; i < 50 && scans.isEmpty(); i++) {
        String currentScans = ts.exec("listscans", true);
        log.info("Got output from listscans:\n{}", currentScans);
        String[] lines = currentScans.split("\n");
        for (int scanOffset = 2; scanOffset < lines.length; scanOffset++) {
          String currentScan = lines[scanOffset];
          if (currentScan.contains(table)) {
            log.info("Retaining scan: {}", currentScan);
            scans.add(currentScan);
          } else {
            log.info("Ignoring scan because of wrong table: {}", currentScan);
          }
        }
        sleepUninterruptibly(300, TimeUnit.MILLISECONDS);
      }
      thread.join();

      assertFalse("Could not find any active scans over table " + table, scans.isEmpty());

      for (String scan : scans) {
        if (!scan.contains("RUNNING")) {
          log.info("Ignoring scan because it doesn't contain 'RUNNING': {}", scan);
          continue;
        }
        String[] parts = scan.split("\\|");
        assertEquals("Expected 14 colums, but found " + parts.length + " instead for '"
            + Arrays.toString(parts) + "'", 14, parts.length);
        String tserver = parts[0].trim();
        // TODO: any way to tell if the client address is accurate? could be local IP, host,
        // loopback...?
        String hostPortPattern = ".+:\\d+";
        assertTrue(tserver.matches(hostPortPattern));
        assertTrue(accumuloClient.instanceOperations().getTabletServers().contains(tserver));
        String client = parts[1].trim();
        assertTrue(client + " does not match " + hostPortPattern, client.matches(hostPortPattern));
        // Scan ID should be a long (throwing an exception if it fails to parse)
        Long.parseLong(parts[11].trim());
      }
    }
    ts.exec("deletetable -f " + table, true);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @Test
  public void testPertableClasspath() throws Exception {
    final String table = name.getMethodName();

    File fooFilterJar = File.createTempFile("FooFilter", ".jar", new File(rootPath));

    FileUtils.copyInputStreamToFile(this.getClass().getResourceAsStream("/FooFilter.jar"),
        fooFilterJar);
    fooFilterJar.deleteOnExit();

    File fooConstraintJar = File.createTempFile("FooConstraint", ".jar", new File(rootPath));
    FileUtils.copyInputStreamToFile(this.getClass().getResourceAsStream("/FooConstraint.jar"),
        fooConstraintJar);
    fooConstraintJar.deleteOnExit();

    ts.exec("config -s " + VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1=" + fooFilterJar.toURI()
        + "," + fooConstraintJar.toURI(), true);

    ts.exec("createtable " + table, true);
    ts.exec("config -t " + table + " -s " + Property.TABLE_CLASSLOADER_CONTEXT.getKey() + "=cx1",
        true);

    sleepUninterruptibly(200, TimeUnit.MILLISECONDS);

    // We can't use the setiter command as Filter implements OptionDescriber which
    // forces us to enter more input that I don't know how to input
    // Instead, we can just manually set the property on the table.
    ts.exec("config -t " + table + " -s " + Property.TABLE_ITERATOR_PREFIX.getKey()
        + "scan.foo=10,org.apache.accumulo.test.FooFilter");

    ts.exec("insert foo f q v", true);

    sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

    ts.exec("scan -np", true, "foo", false);

    ts.exec("constraint -a FooConstraint", true);

    ts.exec("offline -w " + table);
    ts.exec("online -w " + table);

    ts.exec("table " + table, true);
    ts.exec("insert foo f q v", false);
    ts.exec("insert ok foo q v", true);

    ts.exec("deletetable -f " + table, true);
    ts.exec("config -d " + VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1");

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
    // Can't run with Kerberos, can't switch identity in shell presently
    Assume.assumeTrue(getToken() instanceof PasswordToken);
    ts.input.set(getRootPassword() + "\n");
    String err = ts.exec("user NoSuchUser", false);
    assertTrue(err.contains("BAD_CREDENTIALS for user NoSuchUser"));
  }

  @Test
  public void namespaces() throws Exception {
    ts.exec("namespaces", true, "\"\"", true); // default namespace, displayed as quoted empty
                                               // string
    ts.exec("namespaces", true, Namespace.ACCUMULO.name(), true);
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
    ts.input.set("true\n\n\n\nSTRING\n");
    ts.exec("setiter -ns thing2 -scan -class " + SUMMING_COMBINER_ITERATOR + " -p 10 -n name",
        true);
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
    ts.exec("constraint -ns thing3 -a org.apache.accumulo.test.constraints.NumericValueConstraint",
        true);
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
  public void scansWithClassLoaderContext() throws IOException {
    try {
      Class.forName(VALUE_REVERSING_ITERATOR);
      fail("ValueReversingIterator already on the classpath");
    } catch (ClassNotFoundException e) {
      // expected; iterator is already on the class path
    }
    ts.exec("createtable t");
    // Assert that the TabletServer does not know anything about our class
    String result =
        ts.exec("setiter -scan -n reverse -t t -p 21 -class " + VALUE_REVERSING_ITERATOR);
    assertTrue(result.contains("class not found"));
    make10();
    setupFakeContextPath();
    // Add the context to the table so that setiter works.
    result = ts.exec("config -s " + VFS_CONTEXT_CLASSPATH_PROPERTY + FAKE_CONTEXT + "="
        + FAKE_CONTEXT_CLASSPATH);
    assertEquals("root@miniInstance t> config -s " + VFS_CONTEXT_CLASSPATH_PROPERTY + FAKE_CONTEXT
        + "=" + FAKE_CONTEXT_CLASSPATH + "\n", result);

    result = ts
        .exec("config -t t -s " + Property.TABLE_CLASSLOADER_CONTEXT.getKey() + "=" + FAKE_CONTEXT);
    assertEquals("root@miniInstance t> config -t t -s "
        + Property.TABLE_CLASSLOADER_CONTEXT.getKey() + "=" + FAKE_CONTEXT + "\n", result);

    result = ts.exec("setshelliter -pn baz -n reverse -p 21 -class " + VALUE_REVERSING_ITERATOR);
    assertTrue(result.contains("The iterator class does not implement OptionDescriber"));

    // The implementation of ValueReversingIterator in the FAKE context does nothing, the value is
    // not reversed.
    result = ts.exec("scan -pn baz -np -b row1 -e row1");
    assertEquals(2, result.split("\n").length);
    assertTrue(result.contains("value"));
    result = ts.exec("scan -pn baz -np -b row3 -e row5");
    assertEquals(4, result.split("\n").length);
    assertTrue(result.contains("value"));
    result = ts.exec("scan -pn baz -np -r row3");
    assertEquals(2, result.split("\n").length);
    assertTrue(result.contains("value"));
    result = ts.exec("scan -pn baz -np -b row:");
    assertEquals(1, result.split("\n").length);
    result = ts.exec("scan -pn baz -np -b row");
    assertEquals(11, result.split("\n").length);
    assertTrue(result.contains("value"));
    result = ts.exec("scan -pn baz -np -e row:");
    assertEquals(11, result.split("\n").length);
    assertTrue(result.contains("value"));

    setupRealContextPath();
    // Define a new classloader context, but don't set it on the table
    result = ts.exec("config -s " + VFS_CONTEXT_CLASSPATH_PROPERTY + REAL_CONTEXT + "="
        + REAL_CONTEXT_CLASSPATH);
    assertEquals("root@miniInstance t> config -s " + VFS_CONTEXT_CLASSPATH_PROPERTY + REAL_CONTEXT
        + "=" + REAL_CONTEXT_CLASSPATH + "\n", result);
    // Override the table classloader context with the REAL implementation of
    // ValueReversingIterator, which does reverse the value.
    result = ts.exec("scan -pn baz -np -b row1 -e row1 -cc " + REAL_CONTEXT);
    assertEquals(2, result.split("\n").length);
    assertTrue(result.contains("eulav"));
    assertFalse(result.contains("value"));
    result = ts.exec("scan -pn baz -np -b row3 -e row5 -cc " + REAL_CONTEXT);
    assertEquals(4, result.split("\n").length);
    assertTrue(result.contains("eulav"));
    assertFalse(result.contains("value"));
    result = ts.exec("scan -pn baz -np -r row3 -cc " + REAL_CONTEXT);
    assertEquals(2, result.split("\n").length);
    assertTrue(result.contains("eulav"));
    assertFalse(result.contains("value"));
    result = ts.exec("scan -pn baz -np -b row: -cc " + REAL_CONTEXT);
    assertEquals(1, result.split("\n").length);
    result = ts.exec("scan -pn baz -np -b row -cc " + REAL_CONTEXT);
    assertEquals(11, result.split("\n").length);
    assertTrue(result.contains("eulav"));
    assertFalse(result.contains("value"));
    result = ts.exec("scan -pn baz -np -e row: -cc " + REAL_CONTEXT);
    assertEquals(11, result.split("\n").length);
    assertTrue(result.contains("eulav"));
    assertFalse(result.contains("value"));
    ts.exec("deletetable -f t");
  }

  /**
   * The purpose of this test is to verify that you can successfully scan a table with a regular
   * iterator. It was written to verify that the changes made while updating the setshelliter
   * command did not break the existing setiter capabilities. It tests that a table can be scanned
   * with an iterator both while within a table context and also while in the 'notable' context.
   */
  @Test
  public void testScanTableWithIterSetWithoutProfile() throws Exception {
    final String table = name.getMethodName();

    // create a table
    ts.exec("createtable " + table, true);

    // add some data
    ts.exec("insert foo a b c", true);
    ts.exec("scan", true, "foo a:b []\tc");

    // create a normal iterator while in current table context
    ts.input.set("\n1000\n\n");
    ts.exec("setiter -scan -n itname -p 10 -ageoff", true);

    ts.exec("sleep 2", true);
    // scan the created table.
    ts.exec("scan", true, "", true);
    ts.exec("deletetable -f " + table);

    // Repeat process but do it within the 'notable' context (after table creation and insertion)
    // create a table
    ts.exec("createtable " + table, true);

    // add some data
    ts.exec("insert foo a b c", true);
    ts.exec("notable");
    ts.exec("scan -t " + table, true, "foo a:b []\tc");

    // create a normal iterator which in current table context
    ts.input.set("\n1000\n\n");
    ts.exec("setiter -scan -n itname -p 10 -ageoff -t " + table, true);
    ts.exec("sleep 2", true);
    // re-scan the table. Should not see data.
    ts.exec("scan -t " + table, true, "", true);
    ts.exec("deletetable -f " + table);
  }

  /**
   * Validate importdirectory command accepts adding -t tablename option or the accepts original
   * format that uses the current working table. Currently this test does not validate the actual
   * import - only the command syntax.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void importDirectoryCmdFmt() throws Exception {
    final String table = name.getMethodName();

    File importDir = new File(rootPath, "import_" + table);
    assertTrue(importDir.mkdir());
    File errorsDir = new File(rootPath, "errors_" + table);
    assertTrue(errorsDir.mkdir());

    // expect fail - table does not exist.
    ts.exec(String.format("importdirectory -t %s %s %s false", table, importDir, errorsDir), false,
        "TableNotFoundException");

    ts.exec(String.format("table %s", table), false, "TableNotFoundException");

    ts.exec("createtable " + table, true);

    // validate -t option is used.
    ts.exec(String.format("importdirectory -t %s %s %s false", table, importDir, errorsDir), true);

    // validate -t option is used.
    ts.exec(String.format("importdirectory -t %s %s %s false", table, importDir, errorsDir), true);

    // validate -t and -i option is used with new bulk import.
    // This will fail as there are no files in the import directory
    ts.exec(String.format("importdirectory -t %s %s false", table, importDir), false);

    // validate -t and -i option is used with new bulk import.
    // This should pass even if no files in import directory. Empty import dir is ignored.
    ts.exec(String.format("importdirectory -t %s %s false -i", table, importDir), true);

    // validate original cmd format.
    ts.exec(String.format("table %s", table), true);
    ts.exec(String.format("importdirectory %s %s false", importDir, errorsDir), true);

    // expect fail - invalid command,
    ts.exec("importdirectory false", false, "Expected 2 or 3 arguments. There was 1.");

    // expect fail - original cmd without a table.
    ts.exec("notable", true);
    ts.exec(String.format("importdirectory %s %s false", importDir, errorsDir), false,
        "java.lang.IllegalStateException: Not in a table context.");
  }

  private static final String FAKE_CONTEXT = "FAKE";
  private static final String FAKE_CONTEXT_CLASSPATH = "file://" + System.getProperty("user.dir")
      + "/target/" + ShellServerIT.class.getSimpleName() + "-fake-iterators.jar";
  private static final String REAL_CONTEXT = "REAL";
  private static final String REAL_CONTEXT_CLASSPATH = "file://" + System.getProperty("user.dir")
      + "/target/" + ShellServerIT.class.getSimpleName() + "-real-iterators.jar";
  private static final String VALUE_REVERSING_ITERATOR =
      "org.apache.accumulo.test." + "functional.ValueReversingIterator";
  private static final String SUMMING_COMBINER_ITERATOR =
      "org.apache.accumulo.core." + "iterators.user.SummingCombiner";
  private static final String COLUMN_FAMILY_COUNTER_ITERATOR =
      "org.apache.accumulo.core.iterators" + ".ColumnFamilyCounter";

  private void setupRealContextPath() throws IOException {
    // Copy the test iterators jar to tmp
    Path baseDir = new Path(System.getProperty("user.dir"));
    Path targetDir = new Path(baseDir, "target");
    Path jarPath = new Path(targetDir, "TestJar-Iterators.jar");
    Path dstPath = new Path(REAL_CONTEXT_CLASSPATH);
    FileSystem fs = SharedMiniClusterBase.getCluster().getFileSystem();
    fs.copyFromLocalFile(jarPath, dstPath);
  }

  private void setupFakeContextPath() throws IOException {
    // Copy the test iterators jar to tmp
    Path baseDir = new Path(System.getProperty("user.dir"));
    Path targetDir = new Path(baseDir, "target");
    Path classesDir = new Path(targetDir, "classes");
    Path jarPath = new Path(classesDir, "ShellServerIT-iterators.jar");
    Path dstPath = new Path(FAKE_CONTEXT_CLASSPATH);
    FileSystem fs = SharedMiniClusterBase.getCluster().getFileSystem();
    fs.copyFromLocalFile(jarPath, dstPath);
  }

  @Test
  public void whoami() throws Exception {
    AuthenticationToken token = getToken();
    assertTrue(ts.exec("whoami", true).contains(getPrincipal()));
    // Unnecessary with Kerberos enabled, won't prompt for a password
    if (token instanceof PasswordToken) {
      ts.input.set("secret\nsecret\n");
    }
    ts.exec("createuser test_user");
    ts.exec("setauths -u test_user -s 12,3,4");
    String auths = ts.exec("getauths -u test_user");
    assertTrue(auths.contains("3") && auths.contains("12") && auths.contains("4"));
    // No support to switch users within the shell with Kerberos
    if (token instanceof PasswordToken) {
      ts.input.set("secret\n");
      ts.exec("user test_user", true);
      assertTrue(ts.exec("whoami", true).contains("test_user"));
      ts.input.set(getRootPassword() + "\n");
      ts.exec("user root", true);
    }
  }

  private void make10() throws IOException {
    for (int i = 0; i < 10; i++) {
      ts.exec(String.format("insert row%d cf col%d value", i, i));
    }
  }

  private List<String> getFiles(String tableId) throws IOException {
    ts.output.clear();

    ts.exec(
        "scan -t " + MetadataTable.NAME + " -np -c file -b " + tableId + " -e " + tableId + "~");

    log.debug("countFiles(): {}", ts.output.get());

    String[] lines = ts.output.get().split("\n");
    ts.output.clear();

    if (lines.length == 0) {
      return Collections.emptyList();
    }

    return Arrays.asList(Arrays.copyOfRange(lines, 1, lines.length));
  }

  private int countFiles(String tableId) throws IOException {
    return getFiles(tableId).size();
  }

  private String getTableId(String tableName) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      for (int i = 0; i < 5; i++) {
        Map<String,String> nameToId = client.tableOperations().tableIdMap();
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

  private static void assertMatches(String output, String pattern) {
    assertTrue("Pattern " + pattern + " did not match output : " + output, output.matches(pattern));
  }

  private static void assertNotContains(String output, String subsequence) {
    assertFalse("Expected '" + subsequence + "' would not occur in output : " + output,
        output.contains(subsequence));
  }

  @Test
  public void testSummaries() throws Exception {
    ts.exec("createtable summary");
    ts.exec("config -t summary -s table.summarizer.del=" + DeletesSummarizer.class.getName());
    ts.exec("config -t summary -s table.summarizer.fam=" + FamilySummarizer.class.getName());

    ts.exec("addsplits -t summary r1 r2");
    ts.exec("insert r1 f1 q1 v1");
    ts.exec("insert r2 f2 q1 v3");
    ts.exec("insert r2 f2 q2 v4");
    ts.exec("insert r3 f3 q1 v5");
    ts.exec("insert r3 f3 q2 v6");
    ts.exec("insert r3 f3 q3 v7");
    ts.exec("flush -t summary -w");

    String output = ts.exec("summaries");
    assertMatches(output, "(?sm).*^.*deletes\\s+=\\s+0.*$.*");
    assertMatches(output, "(?sm).*^.*total\\s+=\\s+6.*$.*");
    assertMatches(output, "(?sm).*^.*c:f1\\s+=\\s+1.*$.*");
    assertMatches(output, "(?sm).*^.*c:f2\\s+=\\s+2.*$.*");
    assertMatches(output, "(?sm).*^.*c:f3\\s+=\\s+3.*$.*");

    ts.exec("delete r1 f1 q2");
    ts.exec("delete r2 f2 q1");
    ts.exec("flush -t summary -w");

    output = ts.exec("summaries");
    assertMatches(output, "(?sm).*^.*deletes\\s+=\\s+2.*$.*");
    assertMatches(output, "(?sm).*^.*total\\s+=\\s+8.*$.*");
    assertMatches(output, "(?sm).*^.*c:f1\\s+=\\s+1.*$.*");
    assertMatches(output, "(?sm).*^.*c:f2\\s+=\\s+2.*$.*");
    assertMatches(output, "(?sm).*^.*c:f3\\s+=\\s+3.*$.*");

    output = ts.exec("summaries -e r2");
    assertMatches(output, "(?sm).*^.*deletes\\s+=\\s+2.*$.*");
    assertMatches(output, "(?sm).*^.*total\\s+=\\s+5.*$.*");
    assertMatches(output, "(?sm).*^.*c:f1\\s+=\\s+1.*$.*");
    assertMatches(output, "(?sm).*^.*c:f2\\s+=\\s+2.*$.*");
    assertNotContains(output, "c:f3");

    output = ts.exec("summaries -b r2");
    assertMatches(output, "(?sm).*^.*deletes\\s+=\\s+0.*$.*");
    assertMatches(output, "(?sm).*^.*total\\s+=\\s+3.*$.*");
    assertNotContains(output, "c:f1");
    assertNotContains(output, "c:f2");
    assertMatches(output, "(?sm).*^.*c:f3\\s+=\\s+3.*$.*");

    output = ts.exec("summaries -b r1 -e r2");
    assertMatches(output, "(?sm).*^.*deletes\\s+=\\s+1.*$.*");
    assertMatches(output, "(?sm).*^.*total\\s+=\\s+3.*$.*");
    assertNotContains(output, "c:f1");
    assertMatches(output, "(?sm).*^.*c:f2\\s+=\\s+2.*$.*");
    assertNotContains(output, "c:f3");

    output = ts.exec("summaries -sr .*Family.*");
    assertNotContains(output, "deletes ");
    assertMatches(output, "(?sm).*^.*c:f1\\s+=\\s+1.*$.*");
    assertMatches(output, "(?sm).*^.*c:f2\\s+=\\s+2.*$.*");
    assertMatches(output, "(?sm).*^.*c:f3\\s+=\\s+3.*$.*");

    output = ts.exec("summaries -b r1 -e r2 -sr .*Family.*");
    assertNotContains(output, "deletes ");
    assertNotContains(output, "c:f1");
    assertMatches(output, "(?sm).*^.*c:f2\\s+=\\s+2.*$.*");
    assertNotContains(output, "c:f3");
  }

  @Test
  public void testSummarySelection() throws Exception {
    ts.exec("createtable summary2");
    // will create a few files and do not want them compacted
    ts.exec("config -t summary2 -s " + Property.TABLE_MAJC_RATIO + "=10");

    ts.exec("insert r1 f1 q1 v1");
    ts.exec("insert r2 f2 q1 v2");
    ts.exec("flush -t summary2 -w");

    ts.exec("config -t summary2 -s table.summarizer.fam=" + FamilySummarizer.class.getName());

    ts.exec("insert r1 f2 q1 v3");
    ts.exec("insert r3 f3 q1 v4");
    ts.exec("flush -t summary2 -w");

    String output = ts.exec("summaries");
    assertNotContains(output, "c:f1");
    assertMatches(output, "(?sm).*^.*c:f2\\s+=\\s+1.*$.*");
    assertMatches(output, "(?sm).*^.*c:f3\\s+=\\s+1.*$.*");
    // check that there are two files, with one missing summary info
    assertMatches(output, "(?sm).*^.*total[:]2[,]\\s+missing[:]1[,]\\s+extra[:]0.*$.*");

    // compact only the file missing summary info
    ts.exec("compact -t summary2 --sf-no-summary -w");
    output = ts.exec("summaries");
    assertMatches(output, "(?sm).*^.*c:f1\\s+=\\s+1.*$.*");
    assertMatches(output, "(?sm).*^.*c:f2\\s+=\\s+2.*$.*");
    assertMatches(output, "(?sm).*^.*c:f3\\s+=\\s+1.*$.*");
    // check that there are two files, with none missing summary info
    assertMatches(output, "(?sm).*^.*total[:]2[,]\\s+missing[:]0[,]\\s+extra[:]0.*$.*");

    // create a situation where files has summary data outside of tablet
    ts.exec("addsplits -t summary2 r2");
    output = ts.exec("summaries -e r2");
    assertMatches(output, "(?sm).*^.*c:f1\\s+=\\s+1.*$.*");
    assertMatches(output, "(?sm).*^.*c:f2\\s+=\\s+2.*$.*");
    assertMatches(output, "(?sm).*^.*c:f3\\s+=\\s+1.*$.*");
    // check that there are two files, with one having extra summary info
    assertMatches(output, "(?sm).*^.*total[:]2[,]\\s+missing[:]0[,]\\s+extra[:]1.*$.*");

    // compact only the files with extra summary info
    ts.exec("compact -t summary2 --sf-extra-summary -w");
    output = ts.exec("summaries -e r2");
    assertMatches(output, "(?sm).*^.*c:f1\\s+=\\s+1.*$.*");
    assertMatches(output, "(?sm).*^.*c:f2\\s+=\\s+2.*$.*");
    assertNotContains(output, "c:f3");
    // check that there are two files, with none having extra summary info
    assertMatches(output, "(?sm).*^.*total[:]2[,]\\s+missing[:]0[,]\\s+extra[:]0.*$.*");
  }

  @Test
  public void testCreateTableWithLocalityGroups() throws Exception {
    final String table = name.getMethodName();
    ts.exec("createtable " + table + " -l locg1=fam1,fam2", true);
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,Set<Text>> lMap = accumuloClient.tableOperations().getLocalityGroups(table);
      Set<Text> expectedColFams = new HashSet<>(Arrays.asList(new Text("fam1"), new Text("fam2")));
      for (Entry<String,Set<Text>> entry : lMap.entrySet()) {
        assertEquals("locg1", entry.getKey());
        assertTrue(entry.getValue().containsAll(expectedColFams));
      }
      ts.exec("deletetable -f " + table);
    }
  }

  /**
   * Due to the existing complexity of the createtable command, the createtable help only displays
   * an example of setting one locality group. It is possible to set multiple groups if needed. This
   * test verifies that capability.
   */
  @Test
  public void testCreateTableWithMultipleLocalityGroups() throws Exception {
    final String table = name.getMethodName();
    ts.exec("createtable " + table + " -l locg1=fam1,fam2 locg2=colfam1", true);
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,Set<Text>> lMap = accumuloClient.tableOperations().getLocalityGroups(table);
      assertTrue(lMap.containsKey("locg1"));
      assertTrue(lMap.containsKey("locg2"));
      Set<Text> expectedColFams1 = new HashSet<>(Arrays.asList(new Text("fam1"), new Text("fam2")));
      Set<Text> expectedColFams2 = new HashSet<>(Arrays.asList(new Text("colfam1")));
      assertTrue(lMap.get("locg1").containsAll(expectedColFams1));
      assertTrue(lMap.get("locg2").containsAll(expectedColFams2));
      ts.exec("deletetable -f " + table);
    }
  }

  @Test
  public void testCreateTableWithLocalityGroupsBadArguments() throws IOException {
    final String table = name.getMethodName();
    ts.exec("createtable " + table + " -l locg1 fam1,fam2", false);
    ts.exec("createtable " + table + "-l", false);
    ts.exec("createtable " + table + " -l locg1 = fam1,fam2", false);
    ts.exec("createtable " + table + " -l locg1=fam1 ,fam2", false);
    ts.exec("createtable " + table + " -l locg1=fam1,fam2 locg1=fam3,fam4", false);
    ts.exec("createtable " + table + " -l locg1=fam1,fam2 locg2=fam1", false);
    ts.exec("createtable " + table + " -l locg1", false);
    ts.exec("createtable " + table + " group=fam1", false);
    ts.exec("createtable " + table + "-l fam1,fam2", false);
  }

  @Test
  public void testCreateTableWithIterators() throws Exception {
    final String tmpTable = "tmpTable";
    final String table = name.getMethodName();

    // create iterator profile
    // Will use tmpTable for creating profile since setshelliter is requiring a table
    // even though its command line help indicates that it is optional. Likely due to
    // the fact that setshelliter extends setiter, which does require a table argument.
    ts.exec("createtable " + tmpTable, true);
    String output = ts.exec("tables");
    assertTrue(output.contains(tmpTable));

    ts.input.set("\n5000\n\n");
    ts.exec("setshelliter -n itname -p 10 -pn profile1 -ageoff", true);
    output = ts.exec("listshelliter");
    assertTrue(output.contains("Profile : profile1"));

    // create table making use of the iterator profile
    ts.exec("createtable " + table + " -i profile1:scan,minc", true);
    ts.exec("insert foo a b c", true);
    ts.exec("scan", true, "foo a:b []\tc");
    ts.exec("sleep 6", true);
    ts.exec("scan", true, "", true);
    ts.exec("deletetable -f " + table);
    ts.exec("deletetable -f " + tmpTable);
  }

  /**
   * Due to the existing complexity of the createtable command, the createtable help only displays
   * an example of setting one iterator upon table creation. It is possible to set multiple if
   * needed. This test verifies that capability.
   */
  @Test
  public void testCreateTableWithMultipleIterators() throws Exception {
    final String tmpTable = "tmpTable";
    final String table = name.getMethodName();

    // create iterator profile
    // Will use tmpTable for creating profile since setshelliter is requiring a table
    // even though its command line help indicates that it is optional. Likely due to
    // the fact that setshelliter extends setiter, which does require a table argument.
    ts.exec("createtable " + tmpTable, true);
    String output = ts.exec("tables");
    assertTrue(output.contains(tmpTable));

    ts.input.set("\n5000\n\n");
    ts.exec("setshelliter -n itname -p 10 -pn profile1 -ageoff", true);
    output = ts.exec("listshelliter");
    assertTrue(output.contains("Profile : profile1"));

    ts.input.set("2\n");
    ts.exec("setshelliter -n iter2 -p 11 -pn profile2 -vers", true);
    output = ts.exec("listshelliter");
    assertTrue(output.contains("Profile : profile2"));

    // create table making use of the iterator profiles
    ts.exec("createtable " + table + " -i profile1:scan,minc profile2:all ", true);
    ts.exec("insert foo a b c", true);
    ts.exec("scan", true, "foo a:b []\tc");
    ts.exec("sleep 6", true);
    ts.exec("scan", true, "", true);
    output = ts.exec("listiter -t " + table + " -all");
    assertTrue(output.contains("Iterator itname, scan scope options"));
    assertTrue(output.contains("Iterator itname, minc scope options"));
    assertFalse(output.contains("Iterator itname, majc scope options"));
    assertTrue(output.contains("Iterator iter2, scan scope options"));
    assertTrue(output.contains("Iterator iter2, minc scope options"));
    assertTrue(output.contains("Iterator iter2, majc scope options"));
    ts.exec("deletetable -f " + table);
    ts.exec("deletetable -f " + tmpTable);
  }

  @Test
  public void testCreateTableWithIteratorsBadArguments() throws IOException {
    final String tmpTable = "tmpTable";
    final String table = name.getMethodName();
    ts.exec("createtable " + tmpTable, true);
    String output = ts.exec("tables");
    assertTrue(output.contains(tmpTable));
    ts.input.set("\n5000\n\n");
    ts.exec("setshelliter -n itname -p 10 -pn profile1 -ageoff", true);
    output = ts.exec("listshelliter");
    assertTrue(output.contains("Profile : profile1"));
    // test various bad argument calls
    ts.exec("createtable " + table + " -i noprofile:scan,minc", false);
    ts.exec("createtable " + table + " -i profile1:scan,minc,all,majc", false);
    ts.exec("createtable " + table + " -i profile1:scan,all,majc", false);
    ts.exec("createtable " + table + " -i profile1:scan,min,majc", false);
    ts.exec("createtable " + table + " -i profile1:scan,max,all", false);
    ts.exec("createtable " + table + " -i profile1:", false);
    ts.exec("createtable " + table + " -i profile1: ", false);
    ts.exec("createtable " + table + " -i profile1:-scan", false);
    ts.exec("createtable " + table + " profile1:majc", false);
    ts.exec("createtable " + table + " -i profile1: all", false);
    ts.exec("createtable " + table + " -i profile1: All", false);
    ts.exec("createtable " + table + " -i profile1: scan", false);
    ts.exec("createtable " + table + " -i profile1:minc scan", false);
    ts.exec("createtable " + table + " -i profile1:minc,Scan", false);
    ts.exec("createtable " + table + " -i profile1:minc, scan", false);
    ts.exec("createtable " + table + " -i profile1:minc,,scan", false);
    ts.exec("createtable " + table + " -i profile1:minc,minc", false);
    ts.exec("createtable " + table + " -i profile1:minc,Minc", false);
    ts.exec("createtable " + table + " -i profile1:minc, ,scan", false);
    ts.exec("createtable " + table + "-i", false);
    ts.exec("createtable " + table + "-i ", false);
    ts.exec("deletetable -f " + tmpTable);
  }

  /**
   * Verify that table can be created in offline status and then be brought online.
   */
  @Test
  public void testCreateTableOffline() throws IOException {
    final String tableName = name.getMethodName() + "_table";
    ts.exec("createtable " + tableName + " -o", true);
    String output = ts.exec("tables");
    assertTrue(output.contains(tableName));
    output = ts.exec("scan -t " + tableName, false, "is offline", true);
    assertTrue(output.contains("TableOfflineException"));
    ts.exec("table " + tableName, true);
    ts.exec("online", true);
    ts.exec("scan", true);
    ts.exec("deletetable -f " + tableName, true);
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, sorted and un-encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithSplitsFile1()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 1000, 12, false, false, true, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, unsorted and un-encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithSplitsFile2()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 300, 12, false, false, false, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, sorted and encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithSplitsFile3()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 100, 23, false, true, true, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, sorted and un-encoded with a blank line and no repeats.
   */
  @Test
  public void testCreateTableWithSplitsFile4()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 100, 31, false, false, true, true, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, sorted and un-encoded with a blank line and no repeats.
   */
  @Test
  public void testCreateTableWithSplitsFile5()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 100, 32, false, false, true, false, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, unsorted and un-encoded with a blank line and repeats.
   */
  @Test
  public void testCreateTableWithSplitsFile6()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 100, 12, false, false, false, true, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, sorted and encoded with a blank line and repeats.
   */
  @Test
  public void testCreateTableWithSplitsFile7()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 100, 12, false, false, true, true, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits file will be empty.
   */
  @Test(expected = org.apache.accumulo.core.client.TableNotFoundException.class)
  public void testCreateTableWithEmptySplitFile()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 0, 0, false, false, false, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, false);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table that used splits from another table.
   */
  @Test
  public void testCreateTableWithCopySplitsFromOtherTable()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    // create a table and add some splits
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName1 = name.getMethodName() + "_table1";
      ts.exec("createtable " + tableName1, true);
      String output = ts.exec("tables", true);
      assertTrue(output.contains(tableName1));
      ts.exec("table " + tableName1, true);
      // add splits to this table using the addsplits command.
      List<Text> splits = new ArrayList<>();
      splits.add(new Text("ccccc"));
      splits.add(new Text("fffff"));
      splits.add(new Text("mmmmm"));
      splits.add(new Text("sssss"));
      ts.exec("addsplits " + splits.get(0) + " " + splits.get(1) + " " + splits.get(2) + " "
          + splits.get(3), true);
      // Now create a table that will used the previous tables splits and create them at table
      // creation
      final String tableName2 = name.getMethodName() + "_table2";
      ts.exec("createtable " + tableName2 + " --copy-splits " + tableName1, true);
      ts.exec("table " + tableName1, true);
      String tablesOutput = ts.exec("tables", true);
      assertTrue(tablesOutput.contains(tableName2));
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName2);
      assertEquals(new TreeSet<>(splits), new TreeSet<>(createdSplits));
      ts.exec("deletetable -f " + tableName1, true);
      ts.exec("deletetable -f " + tableName2, true);
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, sorted and encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile1()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 200, 12, true, true, true, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, unsorted and encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile2()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 300, 12, true, true, false, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, sorted and encoded with no repeats or blank lines.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile3()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 100, 23, true, true, true, false, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, sorted and encoded with a blank line and no repeats.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile4()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 100, 31, true, true, true, true, false);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, sorted and encoded with a blank line and no repeats.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile5()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 100, 32, true, true, true, false, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, unsorted and encoded with a blank line and repeats.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile6()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 100, 12, true, true, false, true, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  /**
   * Use shell to create a table with a supplied file containing splits.
   *
   * The splits will be contained in a file, sorted and encoded with a blank line and repeats.
   */
  @Test
  public void testCreateTableWithBinarySplitsFile7()
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String splitsFile = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      splitsFile = System.getProperty("user.dir") + "/target/splitFile";
      generateSplitsFile(splitsFile, 100, 12, true, true, true, true, true);
      SortedSet<Text> expectedSplits = readSplitsFromFile(splitsFile, false);
      final String tableName = name.getMethodName() + "_table";
      ts.exec("createtable " + tableName + " -sf " + splitsFile, true);
      Collection<Text> createdSplits = client.tableOperations().listSplits(tableName);
      assertEquals(expectedSplits, new TreeSet<>(createdSplits));
    } finally {
      Files.delete(Paths.get(splitsFile));
    }
  }

  private SortedSet<Text> readSplitsFromFile(final String splitsFile, boolean decode)
      throws IOException {
    SortedSet<Text> splits = new TreeSet<>();
    try (BufferedReader reader = newBufferedReader(Paths.get(splitsFile))) {
      String split;
      while ((split = reader.readLine()) != null) {
        Text unencodedString = decode(split, decode);
        if (unencodedString != null)
          splits.add(unencodedString);
      }
    }
    return splits;
  }

  private void generateSplitsFile(final String splitsFile, final int numItems, final int len,
      final boolean binarySplits, final boolean encoded, final boolean sort,
      final boolean addBlankLine, final boolean repeat) throws IOException {

    java.nio.file.Path splitsPath = java.nio.file.Paths.get(splitsFile);
    int insertAt = (len % 2 == 0) ? len / 2 : (len + 1) / 2;
    Collection<Text> sortedSplits = null;
    Collection<Text> randomSplits = null;

    if (binarySplits)
      randomSplits = generateBinarySplits(numItems, len);
    else
      randomSplits = generateNonBinarySplits(numItems, len);

    if (sort)
      sortedSplits = new TreeSet<>(randomSplits);

    try (BufferedWriter writer = Files.newBufferedWriter(splitsPath, UTF_8)) {
      int cnt = 0;
      Collection<Text> splits;
      if (sort)
        splits = sortedSplits;
      else
        splits = randomSplits;

      for (Text text : splits) {
        if (addBlankLine && cnt++ == insertAt)
          writer.write('\n');
        writer.write(encode(text, encoded) + '\n');
        if (repeat)
          writer.write(encode(text, encoded) + '\n');
      }
    }
  }

  private Collection<Text> generateNonBinarySplits(final int numItems, final int len) {
    Set<Text> splits = new HashSet<>();
    for (int i = 0; i < numItems; i++) {
      splits.add(getRandomText(len));
    }
    return splits;
  }

  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "predictable random is okay for testing")
  private Collection<Text> generateBinarySplits(final int numItems, final int len) {
    Set<Text> splits = new HashSet<>();
    Random rand = new Random();
    for (int i = 0; i < numItems; i++) {
      byte[] split = new byte[len];
      rand.nextBytes(split);
      splits.add(new Text(split));
    }
    return splits;
  }

  private Text getRandomText(final int len) {
    int desiredLen = len;
    if (len > 32)
      desiredLen = 32;
    return new Text(
        String.valueOf(UUID.randomUUID()).replaceAll("-", "").substring(0, desiredLen - 1));
  }

  private static String encode(final Text text, final boolean encode) {
    if (text.toString().isBlank())
      return null;
    return encode ? Base64.getEncoder().encodeToString(TextUtil.getBytes(text)) : text.toString();
  }

  private Text decode(final String text, final boolean decode) {
    if (requireNonNull(text).isBlank())
      return null;
    return decode ? new Text(Base64.getDecoder().decode(text)) : new Text(text);
  }
}
