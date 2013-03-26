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
package org.apache.accumulo.core.util.shell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import jline.ConsoleReader;

import org.apache.accumulo.core.util.format.DateStringFormatter;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

public class ShellTest {
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
  
  private TestOutputStream output;
  private Shell shell;
  
  void exec(String cmd) throws IOException {
    output.clear();
    shell.execCommand(cmd, true, true);
  }
  
  void exec(String cmd, boolean expectGoodExit) throws IOException {
    exec(cmd);
    if (expectGoodExit)
      assertGoodExit("", true);
    else
      assertBadExit("", true);
  }
  
  void exec(String cmd, boolean expectGoodExit, String expectString) throws IOException {
    exec(cmd, expectGoodExit, expectString, true);
  }
  
  void exec(String cmd, boolean expectGoodExit, String expectString, boolean stringPresent) throws IOException {
    exec(cmd);
    if (expectGoodExit)
      assertGoodExit(expectString, stringPresent);
    else
      assertBadExit(expectString, stringPresent);
  }
  
  @Before
  public void setup() throws IOException {
    Shell.log.setLevel(Level.OFF);
    output = new TestOutputStream();
    shell = new Shell(new ConsoleReader(new FileInputStream(FileDescriptor.in), new OutputStreamWriter(output)));
    shell.setLogErrorsToConsole();
    shell.config("--fake", "-p", "pass");
  }
  
  void assertGoodExit(String s, boolean stringPresent) {
    Shell.log.debug(output.get());
    assertEquals(shell.getExitCode(), 0);
    if (s.length() > 0)
      assertEquals(s + " present in " + output.get() + " was not " + stringPresent, stringPresent, output.get().contains(s));
  }
  
  void assertBadExit(String s, boolean stringPresent) {
    Shell.log.debug(output.get());
    assertTrue(shell.getExitCode() > 0);
    if (s.length() > 0)
      assertEquals(s + " present in " + output.get() + " was not " + stringPresent, stringPresent, output.get().contains(s));
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
    exec("deletetable test -f", true, "Table: [test] has been deleted");
  }
  
  @Test
  public void insertDeleteScanTest() throws IOException {
    Shell.log.debug("Starting insertDeleteScan test ------------------------");
    exec("insert r f q v", false, "java.lang.IllegalStateException: Not in a table context");
    exec("delete r f q", false, "java.lang.IllegalStateException: Not in a table context");
    exec("createtable test", true);
    exec("insert r f q v", true);
    exec("scan", true, "r f:q []    v");
    exec("delete r f q", true);
    exec("scan", true, "r f:q []    v", false);
    exec("insert \\x90 \\xa0 \\xb0 \\xc0\\xd0\\xe0\\xf0", true);
    exec("scan", true, "\\x90 \\xA0:\\xB0 []    \\xC0\\xD0");
    exec("scan -f 2", true, "\\x90 \\xA0:\\xB0 []    \\xC0\\xD0");
    exec("scan -f 2", true, "\\x90 \\xA0:\\xB0 []    \\xC0\\xD0\\xE0", false);
    exec("scan -b \\x90 -e \\x90 -c \\xA0", true, "\\x90 \\xA0:\\xB0 []    \\xC0");
    exec("scan -b \\x90 -e \\x90 -c \\xA0:\\xB0", true, "\\x90 \\xA0:\\xB0 []    \\xC0");
    exec("scan -b \\x90 -be", true, "\\x90 \\xA0:\\xB0 []    \\xC0", false);
    exec("scan -e \\x90 -ee", true, "\\x90 \\xA0:\\xB0 []    \\xC0", false);
    exec("scan -b \\x90\\x00", true, "\\x90 \\xA0:\\xB0 []    \\xC0", false);
    exec("scan -e \\x8f", true, "\\x90 \\xA0:\\xB0 []    \\xC0", false);
    exec("delete \\x90 \\xa0 \\xb0", true);
    exec("scan", true, "\\x90 \\xA0:\\xB0 []    \\xC0", false);
    exec("deletetable test -f", true, "Table: [test] has been deleted");
  }
  
  @Test
  public void authsTest() throws Exception {
    Shell.log.debug("Starting auths test --------------------------");
    exec("setauths x,y,z", false, "Missing required option");
    exec("setauths -s x,y,z -u notauser", false, "user does not exist");
    exec("setauths -s y,z,x", true);
    exec("getauths -u notauser", false,"user does not exist");
    exec("getauths", true,"x,y,z");
    exec("addauths -u notauser", false,"Missing required option");
    exec("addauths -u notauser -s foo", false,"user does not exist");
    exec("addauths -s a", true);
    exec("getauths", true, "a,x,y,z");
    exec("setauths -c", true);
  }
  
  @Test
  public void userTest() throws Exception {
    Shell.log.debug("Starting user test --------------------------");
    //exec("createuser root", false, "user exists");
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
  public void scanDateStringFormatterTest() throws IOException {
    Shell.log.debug("Starting scan dateStringFormatter test --------------------------");
    exec("createtable t", true);
    exec("insert r f q v -ts 0", true);
    DateFormat dateFormat = new SimpleDateFormat(DateStringFormatter.DATE_FORMAT);
    String expected = String.format("r f:q [] %s    v", dateFormat.format(new Date(0)));
    exec("scan -fm org.apache.accumulo.core.util.format.DateStringFormatter -st", true, expected);
    exec("deletetable t -f", true, "Table: [t] has been deleted");
  }
}
