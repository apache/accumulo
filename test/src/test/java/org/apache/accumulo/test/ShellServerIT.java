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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map.Entry;

import jline.console.ConsoleReader;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
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
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.server.trace.TraceServer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.DistCp;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ShellServerIT {
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
  
  private static String secret = "superSecret";
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  public static TestOutputStream output;
  public static StringInputStream input;
  public static Shell shell;
  private static Process traceProcess;
  
  static String exec(String cmd) throws IOException {
    output.clear();
    shell.execCommand(cmd, true, true);
    return output.get();
  }
  
  static String exec(String cmd, boolean expectGoodExit) throws IOException {
    String result = exec(cmd);
    if (expectGoodExit)
      assertGoodExit("", true);
    else
      assertBadExit("", true);
    return result;
  }
  
  static String exec(String cmd, boolean expectGoodExit, String expectString) throws IOException {
    return exec(cmd, expectGoodExit, expectString, true);
  }
  
  static String exec(String cmd, boolean expectGoodExit, String expectString, boolean stringPresent) throws IOException {
    String result = exec(cmd);
    if (expectGoodExit)
      assertGoodExit(expectString, stringPresent);
    else
      assertBadExit(expectString, stringPresent);
    return result;
  }
  
  static void assertGoodExit(String s, boolean stringPresent) {
    Shell.log.info(output.get());
    assertEquals(0, shell.getExitCode());
    
    if (s.length() > 0)
      assertEquals(s + " present in " + output.get() + " was not " + stringPresent, stringPresent, output.get().contains(s));
  }
  
  static void assertBadExit(String s, boolean stringPresent) {
    Shell.log.debug(output.get());
    assertTrue(shell.getExitCode() > 0);
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
    
    // history file is updated in $HOME
    System.setProperty("HOME", folder.getRoot().getAbsolutePath());
    
    // start the shell
    output = new TestOutputStream();
    input = new StringInputStream();
    shell = new Shell(new ConsoleReader(input, output));
    shell.setLogErrorsToConsole();
    shell.config("-u", "root", "-p", secret, "-z", cluster.getConfig().getInstanceName(), cluster.getConfig().getZooKeepers());
    exec("quit", true);
    shell.start();
    shell.setExit(false);
    
    // use reflection to call this method so it does not need to be made public
    Method method = cluster.getClass().getDeclaredMethod("exec", Class.class, String[].class);
    method.setAccessible(true);
    traceProcess = (Process) method.invoke(cluster, TraceServer.class, new String[0]);
    
    // give the tracer some time to start
    UtilWaitThread.sleep(1000);
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    traceProcess.destroy();
    folder.delete();
  }
  
  @After
  public void tearDown() throws Exception {
    Connector c = cluster.getConnector("root", secret);
    for (String table : c.tableOperations().list()) {
      if (!table.equals(MetadataTable.NAME) && !table.equals(RootTable.NAME) && !table.equals("trace"))
        c.tableOperations().delete(table);
    }
  }
  
  @Test(timeout = 30*1000)
  public void exporttableImporttable() throws Exception {
    // exporttable / importtable
    exec("createtable t -evc", true);
    make10();
    exec("addsplits row5", true);
    exec("config -t t -s table.split.threshold=345M", true);
    exec("offline t", true);
    String export = "file://" + folder.newFolder().toString();
    exec("exporttable -t t " + export, true);
    DistCp cp = newDistCp();
    String import_ = "file://" + folder.newFolder().toString();
    cp.run(new String[] {"-f", export + "/distcp.txt", import_});
    exec("importtable t2 " + import_, true);
    exec("config -t t2 -np", true, "345M", true);
    exec("getsplits -t t2", true, "row5", true);
    exec("constraint --list -t t2", true, "VisibilityConstraint=2", true);
    exec("onlinetable t", true);
    exec("deletetable -f t", true);
    exec("deletetable -f t2", true);
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
  
  @Test(timeout = 30 * 1000)
  public void setscaniterDeletescaniter() throws Exception {
    // setscaniter, deletescaniter
    exec("createtable t");
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    input.set("true\n\n\nSTRING");
    exec("setscaniter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -n name", true);
    exec("scan", true, "3", true);
    exec("deletescaniter -n name", true);
    exec("scan", true, "1", true);
    exec("deletetable -f t");
    
  }
  
  @Test(timeout = 30 * 1000)
  public void execfile() throws Exception {
    // execfile
    File file = folder.newFile();
    PrintWriter writer = new PrintWriter(file.getAbsolutePath());
    writer.println("about");
    writer.close();
    exec("execfile " + file.getAbsolutePath(), true, Constants.VERSION, true);
    
  }
  
  @Test(timeout = 30 * 1000)
  public void egrep() throws Exception {
    // egrep
    exec("createtable t");
    make10();
    String lines = exec("egrep row[123]", true);
    assertTrue(lines.split("\n").length - 1 == 3);
    exec("deletetable -f t");
  }
  
  @Test(timeout = 30 * 1000)
  public void du() throws Exception {
    // du
    exec("createtable t");
    make10();
    exec("flush -t t -w");
    exec("du t", true, " [t]", true);
    output.clear();
    shell.execCommand("du -h", false, false);
    String o = output.get();
    assertTrue(o.matches(".*26[0-9]\\s\\[t\\]\\n")); // for some reason, there's 1-2 bytes of fluctuation
    exec("deletetable -f t");
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
  
  @Test(timeout = 30 * 1000)
  public void user() throws Exception {
    // createuser, deleteuser, user, users, droptable, grant, revoke
    input.set("secret\nsecret\n");
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
    input.set("secret\nsecret\n");
    exec("user xyzzy", true);
    exec("createtable t", true, "xyzzy@", true);
    exec("insert row1 cf cq 1", true);
    exec("scan", true, "row1", true);
    exec("droptable -f t", true);
    exec("deleteuser xyzzy", false, "delete yourself", true);
    input.set(secret + "\n" + secret + "\n");
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
  
  @Test(timeout = 30 * 1000)
  public void iter() throws Exception {
    // setshelliter, listshelliter, deleteshelliter
    exec("createtable t");
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    input.set("true\n\n\nSTRING\n");
    exec("setshelliter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -pn sum -n name", true);
    exec("setshelliter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 11 -pn sum -n name", false);
    exec("setshelliter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -pn sum -n other", false);
    input.set("true\n\n\nSTRING\n");
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
    exec("deletetable -f t");
    // list iter
    exec("createtable t");
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    exec("insert a cf cq 1");
    input.set("true\n\n\nSTRING\n");
    exec("setiter -scan -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -n name", true);
    exec("setiter -scan -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 11 -n name", false);
    exec("setiter -scan -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -n other", false);
    input.set("true\n\n\nSTRING\n");
    exec("setiter -scan -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 11 -n xyzzy", true);
    exec("scan", true, "3", true);
    exec("listiter -scan", true, "Iterator name", true);
    exec("listiter -scan", true, "Iterator xyzzy", true);
    exec("listiter -minc", true, "Iterator name", false);
    exec("listiter -minc", true, "Iterator xyzzy", false);
    exec("deleteiter -scan -n name", true);
    exec("listiter -scan", true, "Iterator name", false);
    exec("listiter -scan", true, "Iterator xyzzy", true);
    exec("deletetable -f t");
    
  }
  
  @Test(timeout = 30 * 1000)
  public void notable() throws Exception {
    // notable
    exec("createtable xyzzy", true);
    exec("scan", true, " xyzzy>", true);
    assertTrue(output.get().contains(" xyzzy>"));
    exec("notable", true);
    exec("scan", false, "Not in a table context.", true);
    assertFalse(output.get().contains(" xyzzy>"));
    exec("deletetable -f xyzzy");
  }
  
  @Test(timeout = 30 * 1000)
  public void sleep() throws Exception {
    // sleep
    long now = System.currentTimeMillis();
    exec("sleep 0.2", true);
    long diff = System.currentTimeMillis() - now;
    assertTrue(diff >= 200);
    assertTrue(diff < 400);
  }
  
  @Test(timeout = 30 * 1000)
  public void addauths() throws Exception {
    // addauths
    exec("createtable xyzzy -evc");
    exec("insert a b c d -l foo", false, "does not have authorization", true);
    exec("addauths -s foo,bar", true);
    exec("getauths", true, "foo", true);
    exec("getauths", true, "bar", true);
    exec("insert a b c d -l foo");
    exec("scan", true, "[foo]");
    exec("scan -s bar", true, "[foo]", false);
    exec("deletetable -f xyzzy");
  }
  
  @Test(timeout = 30 * 1000)
  public void byeQuitExit() throws Exception {
    // bye, quit, exit
    for (String cmd : "bye quit exit".split(" ")) {
      assertFalse(shell.getExit());
      exec(cmd);
      assertTrue(shell.getExit());
      shell.setExit(false);
    }
  }
  
  @Test(timeout = 30 * 1000)
  public void classpath() throws Exception {
    // classpath
    exec("classpath", true, "Level 2: Java Classloader (loads everything defined by java classpath) URL classpath items are", true);
  }
  
  @Test(timeout = 30 * 1000)
  public void clearCls() throws Exception {
    // clear/cls
    if (shell.getReader().getTerminal().isAnsiSupported()) {
      exec("cls", true, "[1;1H");
      exec("clear", true, "[2J");
    } else {
      exec("cls", false, "does not support");
      exec("clear", false, "does not support");
    }
  }
  
  @Test(timeout = 30 * 1000)
  public void clonetable() throws Exception {
    // clonetable
    exec("createtable orig -evc");
    exec("config -t orig -s table.split.threshold=123M", true);
    exec("addsplits -t orig a b c", true);
    exec("insert a b c value");
    exec("scan", true, "value", true);
    exec("clonetable orig clone");
    // verify constraint, config, and splits were cloned
    exec("constraint --list -t clone", true, "VisibilityConstraint=2", true);
    exec("config -t clone -np", true, "123M", true);
    exec("getsplits -t clone", true, "a\nb\nc\n");
    // compact
    exec("createtable c");
    // make two files
    exec("insert a b c d");
    exec("flush -w");
    exec("insert x y z v");
    exec("flush -w");
    int oldCount = countFiles();
    // merge two files into one
    exec("compact -t c -w");
    assertTrue(countFiles() < oldCount);
    exec("addsplits -t c f");
    // make two more files:
    exec("insert m 1 2 3");
    exec("flush -w");
    exec("insert n 1 2 3");
    exec("flush -w");
    oldCount = countFiles();
    // at this point there are 3 files in the default tablet
    // compact some data:
    exec("compact -b g -e z -w");
    assertTrue(countFiles() == oldCount - 2);
    exec("compact -w");
    assertTrue(countFiles() == oldCount - 2);
    exec("merge --all -t c");
    exec("compact -w");
    assertTrue(countFiles() == oldCount - 3);
    exec("deletetable -f orig");
    exec("deletetable -f clone");
    exec("deletetable -f c");
  }
  
  @Test(timeout = 30 * 1000)
  public void constraint() throws Exception {
    // constraint
    exec("constraint -l -t !METADATA", true, "MetadataConstraints=1", true);
    exec("createtable c -evc");
    exec("constraint -l -t c", true, "VisibilityConstraint=2", true);
    exec("constraint -t c -d 2", true, "Removed constraint 2 from table c");
    exec("constraint -l -t c", true, "VisibilityConstraint=2", false);
    exec("deletetable -f c");
  }
  
  @Test(timeout = 30 * 1000)
  public void deletemany() throws Exception {
    // deletemany
    exec("createtable t");
    make10();
    assertEquals(10, countkeys("t"));
    exec("deletemany -f -b row8");
    assertEquals(8, countkeys("t"));
    exec("scan -t t -np", true, "row8", false);
    make10();
    exec("deletemany -f -b row4 -e row5");
    assertEquals(8, countkeys("t"));
    make10();
    exec("deletemany -f -c cf:col4,cf:col5");
    assertEquals(8, countkeys("t"));
    make10();
    exec("deletemany -f -r row3");
    assertEquals(9, countkeys("t"));
    make10();
    exec("deletemany -f -r row3");
    assertEquals(9, countkeys("t"));
    make10();
    exec("deletemany -f -b row3 -be -e row5 -ee");
    assertEquals(9, countkeys("t"));
    exec("deletetable -f t");
  }
  
  @Test(timeout = 30 * 1000)
  public void deleterows() throws Exception {
    // deleterows
    int base = countFiles();
    exec("createtable t");
    exec("addsplits row5 row7");
    make10();
    exec("flush -w -t t");
    assertTrue(base + 3 == countFiles());
    exec("deleterows -t t -b row5 -e row7", true);
    assertTrue(base + 2 == countFiles());
    exec("deletetable -f t");
  }
  
  @Test(timeout = 30 * 1000)
  public void groups() throws Exception {
    exec("createtable t");
    exec("setgroups -t t alpha=a,b,c num=3,2,1");
    exec("getgroups -t t", true, "alpha=a,b,c", true);
    exec("getgroups -t t", true, "num=1,2,3", true);
    exec("deletetable -f t");
  }
  
  @Test(timeout = 30 * 1000)
  public void grep() throws Exception {
    exec("createtable t", true);
    make10();
    exec("grep row[123]", true, "row1", false);
    exec("grep row5", true, "row5", true);
    exec("deletetable -f t", true);
  }
  
  @Test
  // (timeout = 30 * 1000)
  public void help() throws Exception {
    exec("help -np", true, "Help Commands", true);
    exec("?", true, "Help Commands", true);
    for (String c : ("bye exit quit " + "about help info ? " + "deleteiter deletescaniter listiter setiter setscaniter "
        + "grant revoke systempermissions tablepermissions userpermissions " + "execfile history " + "authenticate cls clear notable sleep table user whoami "
        + "clonetable config createtable deletetable droptable du exporttable importtable offline online renametable tables "
        + "addsplits compact constraint flush getgropus getsplits merge setgroups " + "addauths createuser deleteuser dropuser getauths passwd setauths users "
        + "delete deletemany deleterows egrep formatter interpreter grep importdirectory insert maxrow scan").split(" ")) {
      exec("help " + c, true);
    }
  }
  
  // @Test(timeout = 30 * 1000)
  public void history() throws Exception {
    exec("history -c", true);
    exec("createtable unusualstring");
    exec("deletetable -f unusualstring");
    exec("history", true, "unusualstring", true);
    exec("history", true, "history", true);
  }
  
  @Test(timeout = 30 * 1000)
  public void importDirectory() throws Exception {
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
    assertEquals(0, shell.getExitCode());
    exec("createtable t", true);
    exec("importdirectory " + importDir + " " + errorsDir + " true", true);
    exec("scan -r 00000000", true, "00000000", true);
    exec("scan -r 00000099", true, "00000099", true);
    exec("deletetable -f t");
  }
  
  @Test(timeout = 30 * 1000)
  public void info() throws Exception {
    exec("info", true, Constants.VERSION, true);
  }
  
  @Test(timeout = 30 * 1000)
  public void interpreter() throws Exception {
    exec("createtable t", true);
    exec("interpreter -l", true, "HexScan", false);
    exec("insert \\x02 cf cq value", true);
    exec("scan -b 02", true, "value", false);
    exec("interpreter -i org.apache.accumulo.core.util.interpret.HexScanInterpreter", true);
    exec("interpreter -l", true, "HexScan", true);
    exec("scan -b 02", true, "value", true);
    exec("deletetable -f t", true);
  }
  
  @Test(timeout = 30 * 1000)
  public void listcompactions() throws Exception {
    exec("createtable t", true);
    exec("config -t t -s table.iterator.minc.slow=30,org.apache.accumulo.test.functional.SlowIterator", true);
    exec("config -t t -s table.iterator.minc.slow.opt.sleepTime=100", true);
    exec("insert a cf cq value", true);
    exec("insert b cf cq value", true);
    exec("insert c cf cq value", true);
    exec("insert d cf cq value", true);
    exec("flush -t t", true);
    exec("sleep 0.2", true);
    exec("listcompactions", true, "default_tablet");
    String[] lines = output.get().split("\n");
    String last = lines[lines.length - 1];
    String[] parts = last.split("\\|");
    assertEquals(12, parts.length);
    exec("deletetable -f t", true);
  }
  
  @Test(timeout = 30 * 1000)
  public void maxrow() throws Exception {
    exec("createtable t", true);
    exec("insert a cf cq value", true);
    exec("insert b cf cq value", true);
    exec("insert ccc cf cq value", true);
    exec("insert zzz cf cq value", true);
    exec("maxrow", true, "zzz", true);
    exec("delete zzz cf cq", true);
    exec("maxrow", true, "ccc", true);
    exec("deletetable -f t", true);
  }
  
  @Test(timeout = 30 * 1000)
  public void merge() throws Exception {
    exec("createtable t");
    exec("addsplits a m z");
    exec("getsplits", true, "z", true);
    exec("merge --all", true);
    exec("getsplits", true, "z", false);
    exec("deletetable -f t");
    exec("getsplits -t !METADATA", true);
    assertEquals(2, output.get().split("\n").length);
    exec("getsplits -t !!ROOT", true);
    assertEquals(1, output.get().split("\n").length);
    exec("merge --all -t !METADATA");
    exec("getsplits -t !METADATA", true);
    assertEquals(1, output.get().split("\n").length);
  }
  
  @Test(timeout = 30 * 1000)
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
  
  @Test(timeout = 30 * 1000)
  public void renametable() throws Exception {
    exec("createtable aaaa");
    exec("insert this is a value");
    exec("renametable aaaa xyzzy");
    exec("tables", true, "xyzzy", true);
    exec("tables", true, "aaaa", false);
    exec("scan -t xyzzy", true, "value", true);
    exec("deletetable -f xyzzy", true);
  }
  
  @Test(timeout = 30 * 1000)
  public void systempermission() throws Exception {
    exec("systempermissions");
    assertEquals(8, output.get().split("\n").length - 1);
    exec("tablepermissions", true);
    assertEquals(6, output.get().split("\n").length - 1);
  }
  
  @Test(timeout = 30 * 1000)
  public void listscans() throws Exception {
    exec("createtable t", true);
    exec("config -t t -s table.iterator.scan.slow=30,org.apache.accumulo.test.functional.SlowIterator", true);
    exec("config -t t -s table.iterator.scan.slow.opt.sleepTime=100", true);
    exec("insert a cf cq value", true);
    exec("insert b cf cq value", true);
    exec("insert c cf cq value", true);
    exec("insert d cf cq value", true);
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          Connector connector = cluster.getConnector("root", secret);
          Scanner s = connector.createScanner("t", Authorizations.EMPTY);
          for (@SuppressWarnings("unused")
          Entry<Key,Value> kv : s)
            ;
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    };
    thread.start();
    exec("sleep 0.1", true);
    String scans = exec("listscans", true);
    String lines[] = scans.split("\n");
    String last = lines[lines.length - 1];
    assertTrue(last.contains("RUNNING"));
    String parts[] = last.split("\\|");
    assertEquals(13, parts.length);
    thread.join();
    exec("deletetable -f t", true);
  }
  
  @Test(timeout = 30 * 1000)
  public void testPertableClasspath() throws Exception {
    File fooFilterJar = File.createTempFile("FooFilter", ".jar");
    FileUtils.copyURLToFile(this.getClass().getResource("/FooFilter.jar"), fooFilterJar);
    fooFilterJar.deleteOnExit();
    
    File fooConstraintJar = File.createTempFile("FooConstraint", ".jar");
    FileUtils.copyURLToFile(this.getClass().getResource("/FooConstraint.jar"), fooConstraintJar);
    fooConstraintJar.deleteOnExit();
    
    exec(
        "config -s " + Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1=" + fooFilterJar.toURI().toString() + "," + fooConstraintJar.toURI().toString(),
        true);
    
    exec("createtable ptc", true);
    exec("config -t ptc -s " + Property.TABLE_CLASSPATH.getKey() + "=cx1", true);
    
    UtilWaitThread.sleep(200);
    
    exec("setiter -scan -class org.apache.accumulo.test.FooFilter -p 10 -n foo", true);
    
    exec("insert foo f q v", true);
    
    UtilWaitThread.sleep(100);
    
    exec("scan -np", true, "foo", false);
    
    exec("constraint -a FooConstraint", true);
    
    exec("offline ptc");
    UtilWaitThread.sleep(500);
    exec("online ptc");
    
    exec("table ptc", true);
    exec("insert foo f q v", false);
    exec("insert ok foo q v", true);
    
    exec("deletetable ptc", true);
    exec("config -d " + Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1");
    
  }
  
  @Test(timeout = 30 * 1000)
  public void trace() throws Exception {
    exec("trace on", true);
    exec("createtable t", true);
    exec("insert a b c value", true);
    exec("scan -np", true, "value", true);
    exec("deletetable -f t");
    exec("sleep 1");
    String trace = exec("trace off");
    System.out.println(trace);
    assertTrue(trace.contains("sendMutations"));
    assertTrue(trace.contains("startScan"));
    assertTrue(trace.contains("DeleteTable"));
  }
  
  @Test(timeout=30 * 1000)
  public void badLogin() throws Exception {
    input.set(secret + "\n");
    String err = exec("user NoSuchUser", false);
    assertTrue(err.contains("BAD_CREDENTIALS for user NoSuchUser"));
  }
  
  private int countkeys(String table) throws IOException {
    exec("scan -np -t " + table);
    return output.get().split("\n").length - 1;
  }
  
  @Test(timeout = 30 * 1000)
  public void scans() throws Exception {
    exec("createtable t");
    make10();
    String result = exec("scan -np -b row1 -e row1");
    assertEquals(2, result.split("\n").length);
    result = exec("scan -np -b row3 -e row5");
    assertEquals(4, result.split("\n").length);
    result = exec("scan -np -r row3");
    assertEquals(2, result.split("\n").length);
    result = exec("scan -np -b row:");
    assertEquals(1, result.split("\n").length);
    result = exec("scan -np -b row");
    assertEquals(11, result.split("\n").length);
    result = exec("scan -np -e row:");
    assertEquals(11, result.split("\n").length);
    exec("deletetable -f t");
  }
  
  @Test(timeout = 30 * 1000)
  public void whoami() throws Exception {
    assertTrue(exec("whoami", true).contains("root"));
    input.set("secret\nsecret\n");
    exec("createuser test_user");
    exec("setauths -u test_user -s 12,3,4");
    String auths = exec("getauths -u test_user");
    assertTrue(auths.contains("3") && auths.contains("12") && auths.contains("4"));
    input.set("secret\n");
    exec("user test_user", true);
    assertTrue(exec("whoami", true).contains("test_user"));
    input.set(secret + "\n");
    exec("user root", true);
  }
  
  private void make10() throws IOException {
    for (int i = 0; i < 10; i++) {
      exec(String.format("insert row%d cf col%d value", i, i));
    }
  }
  
  private int countFiles() throws IOException {
    exec("scan -t !METADATA -np -c file");
    return output.get().split("\n").length - 1;
  }
  
}
