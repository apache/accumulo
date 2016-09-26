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
package org.apache.accumulo.core.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.security.auth.DestroyFailedException;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import com.beust.jcommander.JCommander;

public class TestClientOpts {

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Rule
  public TestName testName = new TestName();

  @Test
  public void test() throws Exception {
    BatchWriterConfig cfg = new BatchWriterConfig();

    // document the defaults
    ClientOpts args = new ClientOpts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    BatchScannerOpts bsOpts = new BatchScannerOpts();
    try {
      assertNull(args.getPrincipal());
      fail("Expected to receive exception fetching non-existent principal");
    } catch (AccumuloSecurityException e) {
      // Pass -- no explicit principal and no token to infer a principal from
    }

    assertNull(args.getSecurePassword());
    assertNull(args.getToken());
    assertEquals(Long.valueOf(cfg.getMaxLatency(TimeUnit.MILLISECONDS)), bwOpts.batchLatency);
    assertEquals(Long.valueOf(cfg.getTimeout(TimeUnit.MILLISECONDS)), bwOpts.batchTimeout);
    assertEquals(Long.valueOf(cfg.getMaxMemory()), bwOpts.batchMemory);
    assertFalse(args.debug);
    assertFalse(args.trace);
    assertEquals(10, bsOpts.scanThreads.intValue());
    assertEquals(null, args.instance);
    assertEquals(Authorizations.EMPTY, args.auths);
    assertEquals("localhost:2181", args.zookeepers);
    assertFalse(args.help);

    JCommander jc = new JCommander();
    jc.addObject(args);
    jc.addObject(bwOpts);
    jc.addObject(bsOpts);
    jc.parse("-u", "bar", "-p", "foo", "--batchLatency", "3s", "--batchTimeout", "2s", "--batchMemory", "1M", "--debug", "--trace", "--scanThreads", "7", "-i",
        "instance", "--auths", "G1,G2,G3", "-z", "zoohost1,zoohost2", "--help");
    assertEquals("bar", args.getPrincipal());
    assertNull(args.getSecurePassword());
    assertEquals(new PasswordToken("foo"), args.getToken());
    assertEquals(Long.valueOf(3000), bwOpts.batchLatency);
    assertEquals(Long.valueOf(2000), bwOpts.batchTimeout);
    assertEquals(Long.valueOf(1024 * 1024), bwOpts.batchMemory);
    assertTrue(args.debug);
    assertTrue(args.trace);
    assertEquals(7, bsOpts.scanThreads.intValue());
    assertEquals("instance", args.instance);
    assertEquals(new Authorizations("G1", "G2", "G3"), args.auths);
    assertEquals("zoohost1,zoohost2", args.zookeepers);
    assertTrue(args.help);

  }

  @Test
  public void testVolumes() throws IOException {
    File instanceId = tmpDir.newFolder("instance_id");
    File uuid = new File(instanceId, UUID.randomUUID().toString());
    assertTrue(uuid.createNewFile());
    // document the defaults
    ClientOpts args = new ClientOpts();
    File siteXml = tmpDir.newFile(this.getClass().getSimpleName() + "-" + testName.getMethodName() + "-site.xml");
    FileWriter fileWriter = new FileWriter(siteXml);
    fileWriter.append("<configuration>\n");

    fileWriter.append("<property><name>" + Property.INSTANCE_VOLUMES.getKey() + "</name><value>" + tmpDir.getRoot().toURI().toString()
        + "</value></property>\n");
    fileWriter.append("<property><name>" + ClientProperty.INSTANCE_NAME + "</name><value>foo</value></property>\n");

    fileWriter.append("</configuration>\n");
    fileWriter.close();

    JCommander jc = new JCommander();
    jc.addObject(args);

    jc.parse("--site-file", siteXml.getAbsolutePath());

    args.getInstance();
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testInstanceDir() throws IOException {
    File instanceId = tmpDir.newFolder("instance_id");
    assertTrue(instanceId.mkdir() || instanceId.isDirectory());
    File uuid = new File(instanceId, UUID.randomUUID().toString());
    assertTrue(uuid.createNewFile());
    // document the defaults
    ClientOpts args = new ClientOpts();
    File siteXml = tmpDir.newFile(this.getClass().getSimpleName() + "-" + testName.getMethodName() + "-site.xml");
    FileWriter fileWriter = new FileWriter(siteXml);
    fileWriter.append("<configuration>\n");

    fileWriter
        .append("<property><name>" + Property.INSTANCE_DFS_DIR.getKey() + "</name><value>" + tmpDir.getRoot().getAbsolutePath() + "</value></property>\n");
    fileWriter.append("<property><name>" + Property.INSTANCE_DFS_URI.getKey() + "</name><value>file://</value></property>\n");
    fileWriter.append("<property><name>" + ClientProperty.INSTANCE_NAME + "</name><value>foo</value></property>\n");

    fileWriter.append("</configuration>\n");
    fileWriter.close();

    JCommander jc = new JCommander();
    jc.addObject(args);

    jc.parse("--site-file", siteXml.getAbsolutePath());

    args.getInstance();
  }

  @Test
  public void testSsl() {
    ClientOpts args = new ClientOpts();

    JCommander jc = new JCommander();
    jc.addObject(args);
    jc.parse("--ssl");
    assertEquals(true, args.sslEnabled);
  }

  @Test
  public void testSaslWithClientConfig() throws IOException {
    ClientOpts args = new ClientOpts();

    File clientConfFile = tmpDir.newFile();
    FileWriter writer = new FileWriter(clientConfFile);

    try {
      writer.write(String.format("%s=%s\n", ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), "true"));
    } finally {
      writer.close();
    }

    JCommander jc = new JCommander();
    jc.addObject(args);
    jc.parse("--config-file", clientConfFile.getCanonicalPath());
    args.updateKerberosCredentials();

    assertEquals(KerberosToken.CLASS_NAME, args.getTokenClassName());
  }

  @Test
  public void testSasl() {
    ClientOpts args = new ClientOpts();
    JCommander jc = new JCommander();
    jc.addObject(args);
    jc.parse("--sasl");
    assertEquals(true, args.saslEnabled);
  }

  @Test
  public void testEmptyTokenProperties() {
    ClientOpts args = new ClientOpts();

    JCommander jc = new JCommander();
    jc.addObject(args);
    jc.parse("-tc", EmptyToken.class.getName());
    assertEquals(new EmptyToken(), args.getToken());
  }

  @Test
  public void testPrincipalWithSasl() throws IOException {
    ClientOpts args = new ClientOpts();

    File clientConfFile = tmpDir.newFile();

    JCommander jc = new JCommander();
    jc.addObject(args);
    jc.parse("--config-file", clientConfFile.getCanonicalPath(), "--sasl", "-i", "instance_name");

    ClientConfiguration clientConf = args.getClientConfiguration();
    assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));
  }

  /**
   * An authentication token which requires no options
   */
  private static class EmptyToken implements AuthenticationToken {
    public EmptyToken() {}

    @Override
    public void write(DataOutput out) throws IOException {}

    @Override
    public void readFields(DataInput in) throws IOException {}

    @Override
    public void destroy() throws DestroyFailedException {}

    @Override
    public boolean isDestroyed() {
      return false;
    }

    @Override
    public void init(Properties properties) {}

    @Override
    public Set<TokenProperty> getProperties() {
      return null;
    }

    @Override
    public AuthenticationToken clone() {
      return new EmptyToken();
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof EmptyToken;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }
}
