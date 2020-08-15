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
package org.apache.accumulo.shell;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Scanner;

import org.apache.accumulo.shell.ShellOptionsJC.PasswordConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class PasswordConverterTest {

  private class Password {
    @Parameter(names = "--password", converter = PasswordConverter.class)
    String password;
  }

  private String[] argv;
  private Password password;
  private static InputStream realIn;

  @BeforeClass
  public static void saveIn() {
    realIn = System.in;
  }

  @Before
  public void setup() throws IOException {
    argv = new String[] {"--password", ""};
    password = new Password();

    PipedInputStream in = new PipedInputStream();
    PipedOutputStream out = new PipedOutputStream(in);
    OutputStreamWriter osw = new OutputStreamWriter(out);
    osw.write("secret");
    osw.close();

    System.setIn(in);
  }

  @After
  public void teardown() {
    System.setIn(realIn);
  }

  @Test
  public void testPass() {
    String expected = String.valueOf(Math.random());
    argv[1] = "pass:" + expected;
    new JCommander(password).parse(argv);
    assertEquals(expected, password.password);
  }

  @Test
  public void testEnv() {
    String name = System.getenv().keySet().iterator().next();
    argv[1] = "env:" + name;
    new JCommander(password).parse(argv);
    assertEquals(System.getenv(name), password.password);
  }

  @Test
  public void testFile() throws FileNotFoundException {
    argv[1] = "file:pom.xml";
    Scanner scan = new Scanner(new File("pom.xml"));
    String expected = scan.nextLine();
    scan.close();
    new JCommander(password).parse(argv);
    assertEquals(expected, password.password);
  }

  @Test(expected = ParameterException.class)
  public void testNoFile() throws FileNotFoundException {
    argv[1] = "file:doesnotexist";
    new JCommander(password).parse(argv);
  }

  @Test
  public void testStdin() {
    argv[1] = "stdin";
    new JCommander(password).parse(argv);
    assertEquals("stdin", password.password);
  }
}
