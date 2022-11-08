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
package org.apache.accumulo.core.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.SecureRandom;
import java.util.Scanner;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class PasswordConverterTest {

  private static final SecureRandom random = new SecureRandom();

  private class Password {
    @Parameter(names = "--password", converter = ClientOpts.PasswordConverter.class)
    String password;
  }

  private String[] argv;
  private Password password;
  private static InputStream realIn;

  @BeforeAll
  public static void saveIn() {
    realIn = System.in;
  }

  @BeforeEach
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

  @AfterEach
  public void teardown() {
    System.setIn(realIn);
  }

  @Test
  public void testPass() {
    String expected = String.valueOf(random.nextDouble());
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
  public void testFile() throws IOException {
    argv[1] = "file:pom.xml";
    Scanner scan = new Scanner(new File("pom.xml"), UTF_8);
    String expected = scan.nextLine();
    scan.close();
    new JCommander(password).parse(argv);
    assertEquals(expected, password.password);
  }

  @Test
  public void testNoFile() {
    argv[1] = "file:doesnotexist";
    assertThrows(ParameterException.class, () -> new JCommander(password).parse(argv));
  }

  @Test
  public void testStdin() {
    argv[1] = "stdin";
    new JCommander(password).parse(argv);
    assertEquals("stdin", password.password);
  }

  @Test
  public void testPlainText() {
    argv[1] = "passwordString";
    new JCommander(password).parse(argv);
    assertEquals("passwordString", password.password);
  }
}
