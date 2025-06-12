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
package org.apache.accumulo.harness;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.hadoop.minikdc.MiniKdc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Creates a {@link MiniKdc} for tests to use to exercise secure Accumulo
 */
public class TestingKdc {
  private static final Logger log = LoggerFactory.getLogger(TestingKdc.class);

  public static final int NUM_USERS = 10;
  public static final long MAX_TICKET_LIFETIME_MILLIS = 86400000; // one day

  protected MiniKdc kdc = null;
  protected ClusterUser accumuloServerUser = null;
  protected ClusterUser accumuloAdmin = null;
  protected List<ClusterUser> clientPrincipals = null;

  public final String ORG_NAME = "EXAMPLE";
  public final String ORG_DOMAIN = "COM";

  private String hostname;
  private Path keytabDir;
  private boolean started = false;

  public TestingKdc() throws Exception {
    this(computeKdcDir(), computeKeytabDir(), MAX_TICKET_LIFETIME_MILLIS);
  }

  public static Path computeKdcDir() throws IOException {
    Path userDir = Path.of(System.getProperty("user.dir"));
    Path targetDir = userDir.resolve("target");
    if (Files.notExists(targetDir)) {
      Files.createDirectories(targetDir);
    }
    assertTrue(Files.isDirectory(targetDir), "Could not find Maven target directory: " + targetDir);

    // Create the directories: target/kerberos/minikdc
    Path kdcDir = targetDir.resolve("kerberos").resolve("minikdc");

    if (!Files.isDirectory(kdcDir)) {
      Files.createDirectories(kdcDir);
    }

    return kdcDir;
  }

  public static Path computeKeytabDir() throws IOException {
    Path userDir = Path.of(System.getProperty("user.dir"));
    Path targetDir = userDir.resolve("target");
    if (Files.notExists(targetDir)) {
      Files.createDirectories(targetDir);
    }
    assertTrue(Files.isDirectory(targetDir), "Could not find Maven target directory: " + targetDir);

    // Create the directories: target/kerberos/keytabs
    Path keytabDir = targetDir.resolve("kerberos").resolve("keytabs");

    if (!Files.isDirectory(keytabDir)) {
      Files.createDirectories(keytabDir);
    }

    return keytabDir;
  }

  public TestingKdc(Path kdcDir, Path keytabDir) throws Exception {
    this(kdcDir, keytabDir, MAX_TICKET_LIFETIME_MILLIS);
  }

  public TestingKdc(Path kdcDir, Path keytabDir, long maxTicketLifetime) throws Exception {
    requireNonNull(kdcDir, "KDC directory was null");
    requireNonNull(keytabDir, "Keytab directory was null");
    checkArgument(maxTicketLifetime > 0, "Ticket lifetime must be positive");

    this.keytabDir = keytabDir;
    this.hostname = InetAddress.getLocalHost().getCanonicalHostName();

    log.debug("Starting MiniKdc in {} with keytabs in {}", kdcDir, keytabDir);

    Properties kdcConf = MiniKdc.createConf();
    kdcConf.setProperty(MiniKdc.ORG_NAME, ORG_NAME);
    kdcConf.setProperty(MiniKdc.ORG_DOMAIN, ORG_DOMAIN);
    kdcConf.setProperty(MiniKdc.MAX_TICKET_LIFETIME, Long.toString(maxTicketLifetime));
    // kdcConf.setProperty(MiniKdc.DEBUG, "true");
    kdc = new MiniKdc(kdcConf, kdcDir.toFile());
  }

  /**
   * Starts the KDC and creates the principals and their keytabs
   */
  @SuppressFBWarnings(value = {"PATH_TRAVERSAL_IN", "SWL_SLEEP_WITH_LOCK_HELD"},
      justification = "path provided by test; sleep is okay for a brief pause")
  public synchronized void start() throws Exception {
    checkArgument(!started, "KDC was already started");
    kdc.start();
    Thread.sleep(1000);

    // Create the identity for accumulo servers
    Path keyTabDirPath = keytabDir;
    File accumuloKeytab = keyTabDirPath.resolve("accumulo.keytab").toFile();
    String accumuloPrincipal = String.format("accumulo/%s", hostname);

    log.info("Creating Kerberos principal {} with keytab {}", accumuloPrincipal, accumuloKeytab);
    kdc.createPrincipal(accumuloKeytab, accumuloPrincipal);

    accumuloServerUser = new ClusterUser(qualifyUser(accumuloPrincipal), accumuloKeytab);

    // Create the identity for the "root" user
    String rootPrincipal = "root";
    File rootKeytab = keyTabDirPath.resolve(rootPrincipal + ".keytab").toFile();

    log.info("Creating Kerberos principal {} with keytab {}", rootPrincipal, rootKeytab);
    kdc.createPrincipal(rootKeytab, rootPrincipal);

    accumuloAdmin = new ClusterUser(qualifyUser(rootPrincipal), rootKeytab);

    clientPrincipals = new ArrayList<>(NUM_USERS);
    // Create a number of unprivileged users for tests to use
    for (int i = 1; i <= NUM_USERS; i++) {
      String clientPrincipal = "client" + i;
      File clientKeytab = keyTabDirPath.resolve(clientPrincipal + ".keytab").toFile();

      log.info("Creating Kerberos principal {} with keytab {}", clientPrincipal, clientKeytab);
      kdc.createPrincipal(clientKeytab, clientPrincipal);

      clientPrincipals.add(new ClusterUser(qualifyUser(clientPrincipal), clientKeytab));
    }

    started = true;
  }

  public synchronized void stop() {
    checkArgument(started, "KDC is not started");
    kdc.stop();
    started = false;
  }

  /**
   * A directory where the automatically-created keytab files are written
   */
  public Path getKeytabDir() {
    return keytabDir;
  }

  /**
   * A {@link ClusterUser} for Accumulo server processes to use
   */
  public ClusterUser getAccumuloServerUser() {
    checkArgument(started, "The KDC is not started");
    return accumuloServerUser;
  }

  /**
   * A {@link ClusterUser} which is the Accumulo "root" user
   */
  public ClusterUser getRootUser() {
    checkArgument(started, "The KDC is not started");
    return accumuloAdmin;
  }

  /**
   * The {@link ClusterUser} corresponding to the given offset. Represents an unprivileged user.
   *
   * @param offset The offset to fetch credentials for, valid through {@link #NUM_USERS}
   */
  public ClusterUser getClientPrincipal(int offset) {
    checkArgument(started, "Client principal is not initialized, is the KDC started?");
    checkArgument(offset >= 0 && offset < NUM_USERS,
        "Offset is invalid, must be non-negative and less than " + NUM_USERS);
    return clientPrincipals.get(offset);
  }

  /**
   * @see MiniKdc#createPrincipal(File, String...)
   */
  public void createPrincipal(File keytabFile, String... principals) throws Exception {
    checkArgument(started, "KDC is not started");
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
   * @param primary The primary or primary and instance
   */
  public String qualifyUser(String primary) {
    return String.format("%s@%s.%s", primary, getOrgName(), getOrgDomain());
  }
}
