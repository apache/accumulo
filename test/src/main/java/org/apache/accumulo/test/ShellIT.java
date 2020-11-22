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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.accumulo.test.categories.SunnyDayTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jline.console.ConsoleReader;

@Category({MiniClusterOnlyTests.class, SunnyDayTests.class})
public class ShellIT extends SharedMiniClusterBase {

  @Override
  protected int defaultTimeoutSeconds() {
    return 30;
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterClass
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private static final Logger log = LoggerFactory.getLogger(ShellIT.class);

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

  public static class StringInputStream extends InputStream {
    private String source = "";
    private int offset = 0;

    @Override
    public int read() {
      if (offset == source.length()) {
        return '\n';
      } else {
        return source.charAt(offset++);
      }
    }

    public void set(String other) {
      source = other;
      offset = 0;
    }
  }

  private StringInputStream input;
  private TestOutputStream output;
  private Shell shell;
  private File config;

  void execExpectList(String cmd, boolean expecteGoodExit, List<String> expectedStrings)
      throws IOException {
    exec(cmd);
    if (expecteGoodExit) {
      assertGoodExit("", true);
    } else {
      assertBadExit("", true);
    }

    for (String expectedString : expectedStrings) {
      assertTrue(expectedString + " was not present in " + output.get(),
          output.get().contains(expectedString));
    }
  }

  void exec(String cmd) throws IOException {
    output.clear();
    shell.execCommand(cmd, true, true);
  }

  void exec(String cmd, boolean expectGoodExit) throws IOException {
    exec(cmd);
    if (expectGoodExit) {
      assertGoodExit("", true);
    } else {
      assertBadExit("", true);
    }
  }

  void exec(String cmd, boolean expectGoodExit, String expectString) throws IOException {
    exec(cmd, expectGoodExit, expectString, true);
  }

  void exec(String cmd, boolean expectGoodExit, String expectString, boolean stringPresent)
      throws IOException {
    exec(cmd);
    if (expectGoodExit) {
      assertGoodExit(expectString, stringPresent);
    } else {
      assertBadExit(expectString, stringPresent);
    }
  }

  @Before
  public void setupShell() throws IOException {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    output = new TestOutputStream();
    input = new StringInputStream();
    config = Files.createTempFile(null, null).toFile();
    shell = new Shell(new ConsoleReader(input, output));
    shell.setLogErrorsToConsole();
    shell.config("--config-file", config.toString(), "-u", "root", "-p", getRootPassword(), "-zi",
        getCluster().getInstanceName(), "-zh", getCluster().getZooKeepers());
  }

  @After
  public void teardownShell() {
    if (config.exists()) {
      if (!config.delete()) {
        log.error("Unable to delete {}", config);
      }
    }
    shell.shutdown();
  }

  void assertGoodExit(String s, boolean stringPresent) {
    Shell.log.debug("{}", output.get());
    assertEquals(shell.getExitCode(), 0);
    if (!s.isEmpty()) {
      assertEquals(s + " present in " + output.get() + " was not " + stringPresent, stringPresent,
          output.get().contains(s));
    }
  }

  void assertBadExit(String s, boolean stringPresent) {
    Shell.log.debug("{}", output.get());
    assertTrue(shell.getExitCode() > 0);
    if (!s.isEmpty()) {
      assertEquals(s + " present in " + output.get() + " was not " + stringPresent, stringPresent,
          output.get().contains(s));
    }
    shell.resetExitCode();
  }

  @Test
  public void aboutTest() throws IOException {
    Shell.log.debug("Starting about test -----------------------------------");
    exec("about", true, "Shell - Apache Accumulo Interactive Shell");
    exec("about -v", true, "Current user:");
    exec("about arg", false, "java.lang.IllegalArgumentException: Expected 0 arguments");
  }

  @Test
  public void addGetSplitsTest() throws IOException {
    Shell.log.debug("Starting addGetSplits test ----------------------------");
    exec("addsplits arg", false, "java.lang.IllegalStateException: Not in a table context");
    exec("createtable test", true);
    exec("addsplits 1 \\x80", true);
    exec("getsplits", true, "1\n\\x80");
    exec("getsplits -m 1", true, "1");
    exec("getsplits -b64", true, "MQ==\ngA==");
    exec("deletetable test -f", true, "Table: [test] has been deleted");
  }

  @Test
  public void insertDeleteScanTest() throws IOException {
    Shell.log.debug("Starting insertDeleteScan test ------------------------");
    exec("insert r f q v", false, "java.lang.IllegalStateException: Not in a table context");
    exec("delete r f q", false, "java.lang.IllegalStateException: Not in a table context");
    exec("createtable test", true);
    exec("insert r f q v", true);
    exec("scan", true, "r f:q []\tv");
    exec("delete r f q", true);
    exec("scan", true, "r f:q []\tv", false);
    exec("insert \\x90 \\xa0 \\xb0 \\xc0\\xd0\\xe0\\xf0", true);
    exec("scan", true, "\\x90 \\xA0:\\xB0 []\t\\xC0\\xD0");
    exec("scan -f 2", true, "\\x90 \\xA0:\\xB0 []\t\\xC0\\xD0");
    exec("scan -f 2", true, "\\x90 \\xA0:\\xB0 []\t\\xC0\\xD0\\xE0", false);
    exec("scan -b \\x90 -e \\x90 -c \\xA0", true, "\\x90 \\xA0:\\xB0 []\t\\xC0");
    exec("scan -b \\x90 -e \\x90 -c \\xA0:\\xB0", true, "\\x90 \\xA0:\\xB0 []\t\\xC0");
    exec("scan -b \\x90 -be", true, "\\x90 \\xA0:\\xB0 []\t\\xC0", false);
    exec("scan -e \\x90 -ee", true, "\\x90 \\xA0:\\xB0 []\t\\xC0", false);
    exec("scan -b \\x90\\x00", true, "\\x90 \\xA0:\\xB0 []\t\\xC0", false);
    exec("scan -e \\x8f", true, "\\x90 \\xA0:\\xB0 []\t\\xC0", false);
    exec("delete \\x90 \\xa0 \\xb0", true);
    exec("scan", true, "\\x90 \\xA0:\\xB0 []\t\\xC0", false);
    exec("deletetable test -f", true, "Table: [test] has been deleted");
    // Add tests to verify use of --table parameter
    exec("createtable test2", true);
    exec("notable", true);
    exec("insert r f q v", false, "java.lang.IllegalStateException: Not in a table context");
    exec("insert r1 f1 q1 v1 -t test2", true);
    exec("insert r2 f2 q2 v2 --table test2", true);
    exec("delete r1 f1 q1 -t", false,
        "org.apache.commons.cli.MissingArgumentException: Missing argument for option:");
    exec("delete r1 f1 q1 -t  test3", false,
        "org.apache.accumulo.core.client.TableNotFoundException:");
    exec("scan -t test2", true, "r1 f1:q1 []\tv1\nr2 f2:q2 []\tv2");
    exec("delete r1 f1 q1 -t test2", true);
    exec("scan -t test2", true, "r1 f1:q1 []\tv1", false);
    exec("scan -t test2", true, "r2 f2:q2 []\tv2", true);
    exec("delete r2 f2 q2 --table test2", true);
    exec("scan -t test2", true, "r1 f1:q1 []\tv1", false);
    exec("scan -t test2", true, "r2 f2:q2 []\tv2", false);
    exec("deletetable test2 -f", true, "Table: [test2] has been deleted");
  }

  @Test
  public void insertIntoSpecifiedTableTest() throws IOException {
    Shell.log.debug("Starting insertIntoSpecifiedTableTest -----------------");
    // create two tables for insertion tests
    exec("createtable tab1", true);
    exec("createtable tab2", true);
    // insert data into tab2 while in tab2 context
    exec("insert row1 f q tab2", true);
    // insert another with the table and t argument to verify also works
    exec("insert row2 f q tab2 --table tab2", true);
    exec("insert row3 f q tab2 -t tab2", true);
    // leave all table contexts
    exec("notable", true);
    // without option cannot insert when not in a table context, also cannot add to a table
    // using 'accumulo shell -e "insert ...." fron command line due to no table context being set.
    exec("insert row1 f q tab1", false, "java.lang.IllegalStateException: Not in a table context");
    // but using option can insert to a table with tablename option without being in a table context
    exec("insert row1 f q tab1 --table tab1", true);
    exec("insert row4 f q tab2 -t tab2", true);
    exec("table tab2", true);
    // can also insert into another table even if a different table context
    exec("insert row2 f q tab1 -t tab1", true);
    exec("notable", true);
    // must supply a tablename if option is used
    exec("insert row5 f q tab5 --table", false,
        "org.apache.commons.cli.MissingArgumentException: Missing argument for option:");
    exec("insert row5 f q tab5 --t", false,
        "org.apache.commons.cli.AmbiguousOptionException: Ambiguous option: '--t'");
    // verify expected data is in both tables
    exec("scan -t tab1", true, "row1 f:q []\ttab1\nrow2 f:q []\ttab1");
    exec("scan -t tab2", true,
        "row1 f:q []\ttab2\nrow2 f:q []\ttab2\nrow3 f:q []\ttab2\nrow4 f:q []\ttab2");
    // check that if in table context, inserting into a non-existent table does not change context
    exec("createtable tab3", true);
    exec("table tab3", true);
    exec("insert row1 f1 q1 tab3", true);
    exec("insert row2 f2 q2 tab3 --table idontexist", false,
        "org.apache.accumulo.core.client.TableNotFoundException:");
    exec("insert row2 f2 q2 tab3 -t idontexist", false,
        "org.apache.accumulo.core.client.TableNotFoundException:");
    exec("insert row3 f3 q3 tab3", true); // should be able to insert w/o changing tables
    // verify expected data is in tab3
    exec("scan", true, "row1 f1:q1 []\ttab3\nrow3 f3:q3 []\ttab3");
    // cleanup
    exec("deletetable tab1 -f", true, "Table: [tab1] has been deleted");
    exec("deletetable tab2 -f", true, "Table: [tab2] has been deleted");
    exec("deletetable tab3 -f", true, "Table: [tab3] has been deleted");
  }

  @Test
  public void deleteManyTest() throws IOException {
    exec("deletemany", false, "java.lang.IllegalStateException: Not in a table context");
    exec("createtable test", true);
    exec("deletemany", true, "\n");

    exec("insert 0 0 0 0 -ts 0");
    exec("insert 0 0 0 0 -l 0 -ts 0");
    exec("insert 1 1 1 1 -ts 1");
    exec("insert 2 2 2 2 -ts 2");

    // prompts for delete, and rejects by default
    exec("deletemany", true, "[SKIPPED] 0 0:0 []");
    exec("deletemany -r 0", true, "[SKIPPED] 0 0:0 []");
    exec("deletemany -r 0 -f", true, "[DELETED] 0 0:0 []");

    // with auths, can delete the other record
    exec("setauths -s 0");
    exec("deletemany -r 0 -f", true, "[DELETED] 0 0:0 [0]");

    // delete will show the timestamp
    exec("deletemany -r 1 -f -st", true, "[DELETED] 1 1:1 [] 1");

    // DeleteManyCommand has its own Formatter (DeleterFormatter), so it does not honor the -fm flag
    exec("deletemany -r 2 -f -st -fm org.apache.accumulo.core.util.format.DateStringFormatter",
        true, "[DELETED] 2 2:2 [] 2");

    exec("setauths -c ", true);
    exec("deletetable test -f", true, "Table: [test] has been deleted");
  }

  @Test
  public void authsTest() throws Exception {
    Shell.log.debug("Starting auths test --------------------------");
    exec("setauths x,y,z", false, "Missing required option");
    exec("setauths -s x,y,z -u notauser", false, "user does not exist");
    exec("setauths -s y,z,x", true);
    exec("getauths -u notauser", false, "user does not exist");
    execExpectList("getauths", true, Arrays.asList("x", "y", "z"));
    exec("addauths -u notauser", false, "Missing required option");
    exec("addauths -u notauser -s foo", false, "user does not exist");
    exec("addauths -s a", true);
    execExpectList("getauths", true, Arrays.asList("x", "y", "z", "a"));
    exec("setauths -c", true);
  }

  @Test
  public void userTest() {
    Shell.log.debug("Starting user test --------------------------");
    // Test cannot be done via junit because createuser only prompts for password
    // exec("createuser root", false, "user exists");
  }

  @Test
  public void duContextTest() throws Exception {
    Shell.log.debug("Starting du context test --------------------------");
    exec("createtable t", true);
    exec("du", true, "0 [t]");
    exec("deletetable t -f", true, "Table: [t] has been deleted");
  }

  @Test
  public void duTest() throws IOException {
    Shell.log.debug("Starting DU test --------------------------");
    exec("createtable t", true);
    exec("du t", true, "0 [t]");
    exec("deletetable t -f", true, "Table: [t] has been deleted");
  }

  @Test
  public void duPatternTest() throws IOException {
    Shell.log.debug("Starting DU with pattern test --------------------------");
    exec("createtable t", true);
    exec("createtable tt", true);
    exec("du -p t.*", true, "0 [t, tt]");
    exec("deletetable t -f", true, "Table: [t] has been deleted");
    exec("deletetable tt -f", true, "Table: [tt] has been deleted");
  }

  @Test
  public void scanTimestampTest() throws IOException {
    Shell.log.debug("Starting scanTimestamp test ------------------------");
    exec("createtable test", true);
    exec("insert r f q v -ts 0", true);
    exec("scan -st", true, "r f:q [] 0\tv");
    exec("scan -st -f 0", true, " : [] 0\t");
    exec("deletemany -f", true);
    exec("deletetable test -f", true, "Table: [test] has been deleted");
  }

  @Test
  public void scanFewTest() throws IOException {
    Shell.log.debug("Starting scanFew test ------------------------");
    exec("createtable test", true);
    // historically, showing few did not pertain to ColVis or Timestamp
    exec("insert 1 123 123456 -l '12345678' -ts 123456789 1234567890", true);
    exec("setauths -s 12345678", true);
    String expected = "1 123:123456 [12345678] 123456789\t1234567890";
    String expectedFew = "1 123:12345 [12345678] 123456789\t12345";
    exec("scan -st", true, expected);
    exec("scan -st -f 5", true, expectedFew);
    // also prove that BinaryFormatter behaves same as the default
    exec("scan -st -fm org.apache.accumulo.core.util.format.BinaryFormatter", true, expected);
    exec("scan -st -f 5 -fm org.apache.accumulo.core.util.format.BinaryFormatter", true,
        expectedFew);
    exec("setauths -c", true);
    exec("deletetable test -f", true, "Table: [test] has been deleted");
  }

  @Test
  public void scanDateStringFormatterTest() throws IOException {
    Shell.log.debug("Starting scan dateStringFormatter test --------------------------");
    exec("createtable t", true);
    exec("insert r f q v -ts 0", true);
    @SuppressWarnings("deprecation")
    DateFormat dateFormat =
        new SimpleDateFormat(org.apache.accumulo.core.util.format.DateStringFormatter.DATE_FORMAT);
    String expected = String.format("r f:q [] %s\tv", dateFormat.format(new Date(0)));
    // historically, showing few did not pertain to ColVis or Timestamp
    String expectedNoTimestamp = "r f:q []\tv";
    exec("scan -fm org.apache.accumulo.core.util.format.DateStringFormatter -st", true, expected);
    exec("scan -fm org.apache.accumulo.core.util.format.DateStringFormatter -st -f 1000", true,
        expected);
    exec("scan -fm org.apache.accumulo.core.util.format.DateStringFormatter -st -f 5", true,
        expected);
    exec("scan -fm org.apache.accumulo.core.util.format.DateStringFormatter", true,
        expectedNoTimestamp);
    exec("deletetable t -f", true, "Table: [t] has been deleted");
  }

  @Test
  public void grepTest() throws IOException {
    Shell.log.debug("Starting grep test --------------------------");
    exec("grep", false, "java.lang.IllegalStateException: Not in a table context");
    exec("createtable t", true);
    exec("setauths -s vis", true);
    exec("insert r f q v -ts 0 -l vis", true);

    String expected = "r f:q [vis]\tv";
    String expectedTimestamp = "r f:q [vis] 0\tv";
    exec("grep", false, "No terms specified");
    exec("grep non_matching_string", true, "");
    // historically, showing few did not pertain to ColVis or Timestamp
    exec("grep r", true, expected);
    exec("grep r -f 1", true, expected);
    exec("grep r -st", true, expectedTimestamp);
    exec("grep r -st -f 1", true, expectedTimestamp);
    exec("setauths -c", true);
    exec("deletetable t -f", true, "Table: [t] has been deleted");
  }

  @Test
  public void commentTest() throws IOException {
    Shell.log.debug("Starting comment test --------------------------");
    exec("#", true, "Unknown command", false);
    exec("# foo", true, "Unknown command", false);
    exec("- foo", true, "Unknown command", true);
  }

  @Test
  public void execFileTest() throws IOException {
    Shell.log.debug("Starting exec file test --------------------------");
    shell.config("--config-file", config.toString(), "-u", "root", "-p", getRootPassword(), "-zi",
        getCluster().getInstanceName(), "-zh", getCluster().getZooKeepers(), "-f",
        "src/main/resources/shellit.shellit");
    assertEquals(0, shell.start());
    assertGoodExit("Unknown command", false);
  }

  @Test
  public void setIterTest() throws IOException {
    Shell.log.debug("Starting setiter test --------------------------");
    exec("createtable t", true);

    String cmdJustClass = "setiter -class VersioningIterator -p 1";
    exec(cmdJustClass, false, "java.lang.IllegalArgumentException", false);
    exec(cmdJustClass, false, "fully qualified package name", true);

    String cmdFullPackage = "setiter -class o.a.a.foo -p 1";
    exec(cmdFullPackage, false, "java.lang.IllegalArgumentException", false);
    exec(cmdFullPackage, false, "class not found", true);

    String cmdNoOption = "setiter -class java.lang.String -p 1";
    exec(cmdNoOption, false, "loaded successfully but does not implement SortedKeyValueIterator",
        true);

    input.set("\n\n");
    exec(
        "setiter -scan"
            + " -class org.apache.accumulo.core.iterators.ColumnFamilyCounter -p 30 -name foo",
        true);

    input.set("bar\nname value\n");
    exec("setiter -scan -class org.apache.accumulo.core.iterators.ColumnFamilyCounter -p 31", true);

    // TODO can't verify this as config -t fails, functionality verified in ShellServerIT

    exec("deletetable t -f", true, "Table: [t] has been deleted");
  }
}
