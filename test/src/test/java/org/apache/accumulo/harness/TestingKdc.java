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
package org.apache.accumulo.harness;

import java.io.File;
import java.net.InetAddress;
import java.util.Properties;

import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Creates a {@link MiniKdc} for tests to use to exercise secure Accumulo
 */
public class TestingKdc {
  private static final Logger log = LoggerFactory.getLogger(TestingKdc.class);

  protected MiniKdc kdc = null;
  protected File accumuloKeytab = null, clientKeytab = null;
  protected String accumuloPrincipal = null, clientPrincipal = null;

  public final String ORG_NAME = "EXAMPLE", ORG_DOMAIN = "COM";

  private String hostname;
  private File keytabDir;
  private boolean started = false;

  public TestingKdc() throws Exception {
    File targetDir = new File(System.getProperty("user.dir"), "target");
    Assert.assertTrue("Could not find Maven target directory: " + targetDir, targetDir.exists() && targetDir.isDirectory());

    // Create the directories: target/kerberos/{keytabs,minikdc}
    File krbDir = new File(targetDir, "kerberos"), kdcDir = new File(krbDir, "minikdc");
    keytabDir = new File(krbDir, "keytabs");

    keytabDir.mkdirs();
    kdcDir.mkdirs();

    hostname = InetAddress.getLocalHost().getCanonicalHostName();

    Properties kdcConf = MiniKdc.createConf();
    kdcConf.setProperty(MiniKdc.ORG_NAME, ORG_NAME);
    kdcConf.setProperty(MiniKdc.ORG_DOMAIN, ORG_DOMAIN);
    kdc = new MiniKdc(kdcConf, kdcDir);
  }

  /**
   * Starts the KDC and creates the principals and their keytabs
   */
  public synchronized void start() throws Exception {
    Preconditions.checkArgument(!started, "KDC was already started");
    kdc.start();

    accumuloKeytab = new File(keytabDir, "accumulo.keytab");
    clientKeytab = new File(keytabDir, "client.keytab");

    accumuloPrincipal = String.format("accumulo/%s", hostname);
    clientPrincipal = "client";

    log.info("Creating Kerberos principal {} with keytab {}", accumuloPrincipal, accumuloKeytab);
    kdc.createPrincipal(accumuloKeytab, accumuloPrincipal);
    log.info("Creating Kerberos principal {} with keytab {}", clientPrincipal, clientKeytab);
    kdc.createPrincipal(clientKeytab, clientPrincipal);

    accumuloPrincipal = qualifyUser(accumuloPrincipal);
    clientPrincipal = qualifyUser(clientPrincipal);

    started = true;
  }

  public synchronized void stop() throws Exception {
    Preconditions.checkArgument(started, "KDC is not started");
    kdc.stop();
    started = false;
  }

  /**
   * A directory where the automatically-created keytab files are written
   */
  public File getKeytabDir() {
    return keytabDir;
  }

  /**
   * A Kerberos keytab for the Accumulo server processes
   */
  public File getAccumuloKeytab() {
    Preconditions.checkArgument(started, "Accumulo keytab is not initialized, is the KDC started?");
    return accumuloKeytab;
  }

  /**
   * The corresponding principal for the Accumulo service keytab
   */
  public String getAccumuloPrincipal() {
    Preconditions.checkArgument(started, "Accumulo principal is not initialized, is the KDC started?");
    return accumuloPrincipal;
  }

  /**
   * A Kerberos keytab for client use
   */
  public File getClientKeytab() {
    Preconditions.checkArgument(started, "Client keytab is not initialized, is the KDC started?");
    return clientKeytab;
  }

  /**
   * The corresponding principal for the client keytab
   */
  public String getClientPrincipal() {
    Preconditions.checkArgument(started, "Client principal is not initialized, is the KDC started?");
    return clientPrincipal;
  }

  /**
   * @see MiniKdc#createPrincipal(File, String...)
   */
  public void createPrincipal(File keytabFile, String... principals) throws Exception {
    Preconditions.checkArgument(started, "KDC is not started");
    kdc.createPrincipal(keytabFile, principals);
  }

  /**
   * @return the name for the realm
   */
  public String getOrgName() {
    return ORG_NAME;
  }

    /**
   * @return the domain for the realm
   */
  public String getOrgDomain() {
    return ORG_DOMAIN;
  }

  /**
   * Qualify a username (only the primary from the kerberos principal) with the proper realm
   *
   * @param primary
   *          The primary or primary and instance
   */
  public String qualifyUser(String primary) {
    return String.format("%s@%s.%s", primary, getOrgName(), getOrgDomain());
  }
}
