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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.shell.ShellTest.TestOutputStream;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;

import jline.console.ConsoleReader;

public class ShellConfigTest {
  TestOutputStream output;
  Shell shell;
  PrintStream out;
  File config;
  private static final Logger log = LoggerFactory.getLogger(ShellConfigTest.class);

  @Before
  public void setUp() throws Exception {
    Shell.log.setLevel(Level.ERROR);

    out = System.out;
    output = new TestOutputStream();
    System.setOut(new PrintStream(output));
    config = Files.createTempFile(null, null).toFile();

    shell = new Shell(new ConsoleReader(new FileInputStream(FileDescriptor.in), output));
    shell.setLogErrorsToConsole();
  }

  @After
  public void teardown() throws Exception {
    shell.shutdown();
    output.clear();
    System.setOut(out);
    if (config.exists()) {
      if (!config.delete()) {
        log.error("Unable to delete {}", config);
      }
    }
  }

  public String[] args(String... args) {
    // Avoid a locally installed client configuration file from breaking the test
    String[] finalArgs = new String[args.length + 2];
    int i = 0;
    finalArgs[i++] = "--config-file";
    finalArgs[i++] = config.toString();
    for (String arg : args) {
      finalArgs[i++] = arg;
    }
    return finalArgs;
  }

  @Test
  public void testHelp() throws IOException {
    assertFalse(shell.config(args("--help")));
    assertTrue("Did not print usage", output.get().startsWith("Usage"));
  }

  @Test
  public void testBadArg() throws IOException {
    assertFalse(shell.config(args("--bogus")));
    // JCommander versions after 1.60 will cause the Shell to detect the arg as Unrecognized option
    assertTrue("Did not print Error", output.get().startsWith("ERROR"));
    assertTrue("Did not print usage", output.get().contains("Usage"));
  }

  @Test
  public void testTokenWithoutOptions() throws IOException {
    assertFalse(shell.config(args("--fake", "-tc", PasswordToken.class.getName())));
    assertFalse(output.get().contains(ParameterException.class.getName()));
  }

  @Test
  public void testTokenAndOption() throws IOException {
    assertTrue(shell.config(args("--fake", "-tc", PasswordToken.class.getName(), "-u", "foo", "-l", "password=foo")));
  }

  @Test
  public void testTokenAndOptionAndPassword() throws IOException {
    assertFalse(shell.config(args("--fake", "-tc", PasswordToken.class.getName(), "-l", "password=foo", "-p", "bar")));
    assertTrue(output.get().contains(ParameterException.class.getName()));
  }

  /**
   * Tests getting the ZK hosts config value will fail on String parameter, client config and then fall back to Site configuration. SiteConfiguration will get
   * the accumulo-site.xml from the classpath in src/test/resources
   */
  @Test
  public void testZooKeeperHostFallBackToSite() throws Exception {
    ClientConfiguration clientConfig = new ClientConfiguration();
    assertFalse("Client config contains zk hosts", clientConfig.containsKey(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST.getKey()));
    assertEquals("ShellConfigTestZKHostValue", Shell.getZooKeepers(null, clientConfig));
  }

  @Test
  public void testZooKeeperHostFromClientConfig() throws Exception {
    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.withZkHosts("cc_hostname");
    assertEquals("cc_hostname", Shell.getZooKeepers(null, clientConfig));
  }

  @Test
  public void testZooKeeperHostFromOption() throws Exception {
    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.withZkHosts("cc_hostname");
    assertEquals("opt_hostname", Shell.getZooKeepers("opt_hostname", clientConfig));
  }
}
