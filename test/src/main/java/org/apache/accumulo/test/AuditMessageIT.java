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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that Accumulo is outputting audit messages as expected. Since this is using MiniAccumuloCluster, it could take a while if we test everything in
 * isolation. We test blocks of related operations, run the whole test in one MiniAccumulo instance, trying to clean up objects between each test. The
 * MiniAccumuloClusterTest sets up the log4j stuff differently to an installed instance, instead piping everything through stdout and writing to a set location
 * so we have to find the logs and grep the bits we need out.
 */
public class AuditMessageIT extends ConfigurableMacBase {

  private static final String AUDIT_USER_1 = "AuditUser1";
  private static final String AUDIT_USER_2 = "AuditUser2";
  private static final String PASSWORD = "password";
  private static final String OLD_TEST_TABLE_NAME = "apples";
  private static final String NEW_TEST_TABLE_NAME = "oranges";
  private static final String THIRD_TEST_TABLE_NAME = "pears";
  private static final Authorizations auths = new Authorizations("private", "public");

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Override
  public void beforeClusterStart(MiniAccumuloConfigImpl cfg) throws Exception {
    cfg.setNumTservers(1);
  }

  // Must be static to survive Junit re-initialising the class every time.
  private static String lastAuditTimestamp;
  private Connector auditConnector;
  private Connector conn;

  private static long findAuditMessage(ArrayList<String> input, String pattern) {
    return input.stream().filter(s -> s.matches(".*" + pattern + ".*")).count();
  }

  /**
   * Returns a List of Audit messages that have been grep'd out of the MiniAccumuloCluster output.
   *
   * @param stepName
   *          A unique name for the test being executed, to identify the System.out messages.
   * @return A List of the Audit messages, sorted (so in chronological order).
   */
  private ArrayList<String> getAuditMessages(String stepName) throws IOException {
    // ACCUMULO-3144 Make sure we give the processes enough time to flush the write buffer
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for data to be flushed to output streams");
    }

    for (MiniAccumuloClusterImpl.LogWriter lw : getCluster().getLogWriters()) {
      lw.flush();
    }

    // Grab the audit messages
    System.out.println("Start of captured audit messages for step " + stepName);

    ArrayList<String> result = new ArrayList<>();
    File[] files = getCluster().getConfig().getLogDir().listFiles();
    assertNotNull(files);
    for (File file : files) {
      // We want to grab the files called .out
      if (file.getName().contains(".out") && file.isFile() && file.canRead()) {
        try (java.util.Scanner it = new java.util.Scanner(file, UTF_8.name())) {
          while (it.hasNext()) {
            String line = it.nextLine();
            // strip off prefix, because log4j.properties does
            String pattern = ".* \\[" + AuditedSecurityOperation.AUDITLOG.replace("org.apache.", "").replace(".", "[.]") + "\\] .*";
            if (line.matches(pattern)) {
              // Only include the message if startTimestamp is null. or the message occurred after the startTimestamp value
              if ((lastAuditTimestamp == null) || (line.substring(0, 23).compareTo(lastAuditTimestamp) > 0))
                result.add(line);
            }
          }
        }
      }
    }
    Collections.sort(result);

    for (String s : result) {
      System.out.println(s);
    }
    System.out.println("End of captured audit messages for step " + stepName);
    if (result.size() > 0)
      lastAuditTimestamp = (result.get(result.size() - 1)).substring(0, 23);

    return result;
  }

  private void grantEverySystemPriv(Connector conn, String user) throws AccumuloSecurityException, AccumuloException {
    SystemPermission[] arrayOfP = new SystemPermission[] {SystemPermission.SYSTEM, SystemPermission.ALTER_TABLE, SystemPermission.ALTER_USER,
        SystemPermission.CREATE_TABLE, SystemPermission.CREATE_USER, SystemPermission.DROP_TABLE, SystemPermission.DROP_USER};
    for (SystemPermission p : arrayOfP) {
      conn.securityOperations().grantSystemPermission(user, p);
    }
  }

  @Before
  public void resetInstance() throws Exception {
    conn = getConnector();

    removeUsersAndTables();

    // This will set the lastAuditTimestamp for the first test
    getAuditMessages("setup");
  }

  @After
  public void removeUsersAndTables() throws Exception {
    for (String user : Arrays.asList(AUDIT_USER_1, AUDIT_USER_2)) {
      if (conn.securityOperations().listLocalUsers().contains(user)) {
        conn.securityOperations().dropLocalUser(user);
      }
    }

    TableOperations tops = conn.tableOperations();
    for (String table : Arrays.asList(THIRD_TEST_TABLE_NAME, NEW_TEST_TABLE_NAME, OLD_TEST_TABLE_NAME)) {
      if (tops.exists(table)) {
        tops.delete(table);
      }
    }
  }

  @Test
  public void testTableOperationsAudits() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException,
      InterruptedException {

    conn.securityOperations().createLocalUser(AUDIT_USER_1, new PasswordToken(PASSWORD));
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.SYSTEM);
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.CREATE_TABLE);

    // Connect as Audit User and do a bunch of stuff.
    // Testing activity begins here
    auditConnector = getCluster().getConnector(AUDIT_USER_1, new PasswordToken(PASSWORD));
    auditConnector.tableOperations().create(OLD_TEST_TABLE_NAME);
    auditConnector.tableOperations().rename(OLD_TEST_TABLE_NAME, NEW_TEST_TABLE_NAME);
    Map<String,String> emptyMap = Collections.emptyMap();
    Set<String> emptySet = Collections.emptySet();
    auditConnector.tableOperations().clone(NEW_TEST_TABLE_NAME, OLD_TEST_TABLE_NAME, true, emptyMap, emptySet);
    auditConnector.tableOperations().delete(OLD_TEST_TABLE_NAME);
    auditConnector.tableOperations().offline(NEW_TEST_TABLE_NAME);
    auditConnector.tableOperations().delete(NEW_TEST_TABLE_NAME);
    // Testing activity ends here

    ArrayList<String> auditMessages = getAuditMessages("testTableOperationsAudits");

    assertEquals(1, findAuditMessage(auditMessages, "action: createTable; targetTable: " + OLD_TEST_TABLE_NAME));
    assertEquals(1, findAuditMessage(auditMessages, "action: renameTable; targetTable: " + OLD_TEST_TABLE_NAME));
    assertEquals(1, findAuditMessage(auditMessages, "action: cloneTable; targetTable: " + NEW_TEST_TABLE_NAME));
    assertEquals(1, findAuditMessage(auditMessages, "action: deleteTable; targetTable: " + OLD_TEST_TABLE_NAME));
    assertEquals(1, findAuditMessage(auditMessages, "action: offlineTable; targetTable: " + NEW_TEST_TABLE_NAME));
    assertEquals(1, findAuditMessage(auditMessages, "action: deleteTable; targetTable: " + NEW_TEST_TABLE_NAME));

  }

  @Test
  public void testUserOperationsAudits() throws AccumuloSecurityException, AccumuloException, TableExistsException, InterruptedException, IOException {

    conn.securityOperations().createLocalUser(AUDIT_USER_1, new PasswordToken(PASSWORD));
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.SYSTEM);
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.CREATE_USER);
    grantEverySystemPriv(conn, AUDIT_USER_1);

    // Connect as Audit User and do a bunch of stuff.
    // Start testing activities here
    auditConnector = getCluster().getConnector(AUDIT_USER_1, new PasswordToken(PASSWORD));
    auditConnector.securityOperations().createLocalUser(AUDIT_USER_2, new PasswordToken(PASSWORD));

    // It seems only root can grant stuff.
    conn.securityOperations().grantSystemPermission(AUDIT_USER_2, SystemPermission.ALTER_TABLE);
    conn.securityOperations().revokeSystemPermission(AUDIT_USER_2, SystemPermission.ALTER_TABLE);
    auditConnector.tableOperations().create(NEW_TEST_TABLE_NAME);
    conn.securityOperations().grantTablePermission(AUDIT_USER_2, NEW_TEST_TABLE_NAME, TablePermission.READ);
    conn.securityOperations().revokeTablePermission(AUDIT_USER_2, NEW_TEST_TABLE_NAME, TablePermission.READ);
    auditConnector.securityOperations().changeLocalUserPassword(AUDIT_USER_2, new PasswordToken("anything"));
    auditConnector.securityOperations().changeUserAuthorizations(AUDIT_USER_2, auths);
    auditConnector.securityOperations().dropLocalUser(AUDIT_USER_2);
    // Stop testing activities here

    ArrayList<String> auditMessages = getAuditMessages("testUserOperationsAudits");

    // The user is allowed to create this user and it succeeded
    assertEquals(2, findAuditMessage(auditMessages, "action: createUser; targetUser: " + AUDIT_USER_2));
    assertEquals(
        1,
        findAuditMessage(auditMessages, "action: grantSystemPermission; permission: " + SystemPermission.ALTER_TABLE.toString() + "; targetUser: "
            + AUDIT_USER_2));
    assertEquals(
        1,
        findAuditMessage(auditMessages, "action: revokeSystemPermission; permission: " + SystemPermission.ALTER_TABLE.toString() + "; targetUser: "
            + AUDIT_USER_2));
    assertEquals(
        1,
        findAuditMessage(auditMessages, "action: grantTablePermission; permission: " + TablePermission.READ.toString() + "; targetTable: "
            + NEW_TEST_TABLE_NAME));
    assertEquals(
        1,
        findAuditMessage(auditMessages, "action: revokeTablePermission; permission: " + TablePermission.READ.toString() + "; targetTable: "
            + NEW_TEST_TABLE_NAME));
    // changePassword is allowed and succeeded
    assertEquals(2, findAuditMessage(auditMessages, "action: changePassword; targetUser: " + AUDIT_USER_2 + ""));
    assertEquals(1, findAuditMessage(auditMessages, "action: changeAuthorizations; targetUser: " + AUDIT_USER_2 + "; authorizations: " + auths.toString()));

    // allowed to dropUser and succeeded
    assertEquals(2, findAuditMessage(auditMessages, "action: dropUser; targetUser: " + AUDIT_USER_2));
  }

  @Test
  public void testImportExportOperationsAudits() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException,
      IOException, InterruptedException {

    conn.securityOperations().createLocalUser(AUDIT_USER_1, new PasswordToken(PASSWORD));
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.SYSTEM);
    conn.securityOperations().changeUserAuthorizations(AUDIT_USER_1, auths);
    grantEverySystemPriv(conn, AUDIT_USER_1);

    // Connect as Audit User and do a bunch of stuff.
    // Start testing activities here
    auditConnector = getCluster().getConnector(AUDIT_USER_1, new PasswordToken(PASSWORD));
    auditConnector.tableOperations().create(OLD_TEST_TABLE_NAME);

    // Insert some play data
    BatchWriter bw = auditConnector.createBatchWriter(OLD_TEST_TABLE_NAME, new BatchWriterConfig());
    Mutation m = new Mutation("myRow");
    m.put("cf1", "cq1", "v1");
    m.put("cf1", "cq2", "v3");
    bw.addMutation(m);
    bw.close();

    // Prepare to export the table
    File exportDir = new File(getCluster().getConfig().getDir().toString() + "/export");

    auditConnector.tableOperations().offline(OLD_TEST_TABLE_NAME);
    auditConnector.tableOperations().exportTable(OLD_TEST_TABLE_NAME, exportDir.toString());

    // We've exported the table metadata to the MiniAccumuloCluster root dir. Grab the .rf file path to re-import it
    File distCpTxt = new File(exportDir.toString() + "/distcp.txt");
    File importFile = null;

    // Just grab the first rf file, it will do for now.
    String filePrefix = "file:";

    try (java.util.Scanner it = new java.util.Scanner(distCpTxt, UTF_8.name())) {
      while (it.hasNext() && importFile == null) {
        String line = it.nextLine();
        if (line.matches(".*\\.rf")) {
          importFile = new File(line.replaceFirst(filePrefix, ""));
        }
      }
    }
    FileUtils.copyFileToDirectory(importFile, exportDir);
    auditConnector.tableOperations().importTable(NEW_TEST_TABLE_NAME, exportDir.toString());

    // Now do a Directory (bulk) import of the same data.
    auditConnector.tableOperations().create(THIRD_TEST_TABLE_NAME);
    File failDir = new File(exportDir + "/tmp");
    assertTrue(failDir.mkdirs() || failDir.isDirectory());
    auditConnector.tableOperations().importDirectory(THIRD_TEST_TABLE_NAME, exportDir.toString(), failDir.toString(), false);
    auditConnector.tableOperations().online(OLD_TEST_TABLE_NAME);

    // Stop testing activities here

    ArrayList<String> auditMessages = getAuditMessages("testImportExportOperationsAudits");

    assertEquals(1, findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_CREATE_TABLE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME)));
    assertEquals(1,
        findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_ONLINE_OFFLINE_TABLE_AUDIT_TEMPLATE, "offlineTable", OLD_TEST_TABLE_NAME)));
    assertEquals(1,
        findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_EXPORT_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME, exportDir.toString())));
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            String.format(AuditedSecurityOperation.CAN_IMPORT_AUDIT_TEMPLATE, NEW_TEST_TABLE_NAME, filePrefix + exportDir.toString())));
    assertEquals(1, findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_CREATE_TABLE_AUDIT_TEMPLATE, THIRD_TEST_TABLE_NAME)));
    assertEquals(
        1,
        findAuditMessage(
            auditMessages,
            String.format(AuditedSecurityOperation.CAN_BULK_IMPORT_AUDIT_TEMPLATE, THIRD_TEST_TABLE_NAME, filePrefix + exportDir.toString(), filePrefix
                + failDir.toString())));
    assertEquals(1,
        findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_ONLINE_OFFLINE_TABLE_AUDIT_TEMPLATE, "onlineTable", OLD_TEST_TABLE_NAME)));

  }

  @Test
  public void testDataOperationsAudits() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException,
      InterruptedException {

    conn.securityOperations().createLocalUser(AUDIT_USER_1, new PasswordToken(PASSWORD));
    conn.securityOperations().grantSystemPermission(AUDIT_USER_1, SystemPermission.SYSTEM);
    conn.securityOperations().changeUserAuthorizations(AUDIT_USER_1, auths);
    grantEverySystemPriv(conn, AUDIT_USER_1);

    // Connect as Audit User and do a bunch of stuff.
    // Start testing activities here
    auditConnector = getCluster().getConnector(AUDIT_USER_1, new PasswordToken(PASSWORD));
    auditConnector.tableOperations().create(OLD_TEST_TABLE_NAME);

    // Insert some play data
    BatchWriter bw = auditConnector.createBatchWriter(OLD_TEST_TABLE_NAME, new BatchWriterConfig());
    Mutation m = new Mutation("myRow");
    m.put("cf1", "cq1", "v1");
    m.put("cf1", "cq2", "v3");
    bw.addMutation(m);
    bw.close();

    // Start testing activities here
    // A regular scan
    Scanner scanner = auditConnector.createScanner(OLD_TEST_TABLE_NAME, auths);
    for (Map.Entry<Key,Value> entry : scanner) {
      System.out.println("Scanner row: " + entry.getKey() + " " + entry.getValue());
    }
    scanner.close();

    // A batch scan
    BatchScanner bs = auditConnector.createBatchScanner(OLD_TEST_TABLE_NAME, auths, 1);
    bs.fetchColumn(new Text("cf1"), new Text("cq1"));
    bs.setRanges(Arrays.asList(new Range("myRow", "myRow~")));

    for (Map.Entry<Key,Value> entry : bs) {
      System.out.println("BatchScanner row: " + entry.getKey() + " " + entry.getValue());
    }
    bs.close();

    // Delete some data.
    auditConnector.tableOperations().deleteRows(OLD_TEST_TABLE_NAME, new Text("myRow"), new Text("myRow~"));

    // End of testing activities

    ArrayList<String> auditMessages = getAuditMessages("testDataOperationsAudits");
    assertTrue(1 <= findAuditMessage(auditMessages, "action: scan; targetTable: " + OLD_TEST_TABLE_NAME));
    assertTrue(1 <= findAuditMessage(auditMessages, "action: scan; targetTable: " + OLD_TEST_TABLE_NAME));
    assertEquals(1,
        findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CAN_DELETE_RANGE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME, "myRow", "myRow~")));

  }

  @Test
  public void testDeniedAudits() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException,
      InterruptedException {

    // Create our user with no privs
    conn.securityOperations().createLocalUser(AUDIT_USER_1, new PasswordToken(PASSWORD));
    conn.tableOperations().create(OLD_TEST_TABLE_NAME);
    auditConnector = getCluster().getConnector(AUDIT_USER_1, new PasswordToken(PASSWORD));

    // Start testing activities
    // We should get denied or / failed audit messages here.
    // We don't want the thrown exceptions to stop our tests, and we are not testing that the Exceptions are thrown.

    try {
      auditConnector.tableOperations().create(NEW_TEST_TABLE_NAME);
    } catch (AccumuloSecurityException ex) {}
    try {
      auditConnector.tableOperations().rename(OLD_TEST_TABLE_NAME, NEW_TEST_TABLE_NAME);
    } catch (AccumuloSecurityException ex) {}
    try {
      auditConnector.tableOperations().clone(OLD_TEST_TABLE_NAME, NEW_TEST_TABLE_NAME, true, Collections.<String,String> emptyMap(),
          Collections.<String> emptySet());
    } catch (AccumuloSecurityException ex) {}
    try {
      auditConnector.tableOperations().delete(OLD_TEST_TABLE_NAME);
    } catch (AccumuloSecurityException ex) {}
    try {
      auditConnector.tableOperations().offline(OLD_TEST_TABLE_NAME);
    } catch (AccumuloSecurityException ex) {}
    try {
      Scanner scanner = auditConnector.createScanner(OLD_TEST_TABLE_NAME, auths);
      scanner.iterator().next().getKey();
    } catch (RuntimeException ex) {}
    try {
      auditConnector.tableOperations().deleteRows(OLD_TEST_TABLE_NAME, new Text("myRow"), new Text("myRow~"));
    } catch (AccumuloSecurityException ex) {}

    // ... that will do for now.
    // End of testing activities

    ArrayList<String> auditMessages = getAuditMessages("testDeniedAudits");
    assertEquals(1,
        findAuditMessage(auditMessages, "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_CREATE_TABLE_AUDIT_TEMPLATE, NEW_TEST_TABLE_NAME)));
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_RENAME_TABLE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME, NEW_TEST_TABLE_NAME)));
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_CLONE_TABLE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME, NEW_TEST_TABLE_NAME)));
    assertEquals(1,
        findAuditMessage(auditMessages, "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_DELETE_TABLE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME)));
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_ONLINE_OFFLINE_TABLE_AUDIT_TEMPLATE, "offlineTable", OLD_TEST_TABLE_NAME)));
    assertEquals(1, findAuditMessage(auditMessages, "operation: denied;.*" + "action: scan; targetTable: " + OLD_TEST_TABLE_NAME));
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            "operation: denied;.*" + String.format(AuditedSecurityOperation.CAN_DELETE_RANGE_AUDIT_TEMPLATE, OLD_TEST_TABLE_NAME, "myRow", "myRow~")));
  }

  @Test
  public void testFailedAudits() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException,
      InterruptedException {

    // Start testing activities
    // Test that we get a few "failed" audit messages come through when we tell it to do dumb stuff
    // We don't want the thrown exceptions to stop our tests, and we are not testing that the Exceptions are thrown.
    try {
      conn.securityOperations().dropLocalUser(AUDIT_USER_2);
    } catch (AccumuloSecurityException ex) {}
    try {
      conn.securityOperations().revokeSystemPermission(AUDIT_USER_2, SystemPermission.ALTER_TABLE);
    } catch (AccumuloSecurityException ex) {}
    try {
      conn.securityOperations().createLocalUser("root", new PasswordToken("super secret"));
    } catch (AccumuloSecurityException ex) {}
    ArrayList<String> auditMessages = getAuditMessages("testFailedAudits");
    // ... that will do for now.
    // End of testing activities

    // We're permitted to drop this user, but it fails because the user doesn't actually exist.
    assertEquals(2, findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.DROP_USER_AUDIT_TEMPLATE, AUDIT_USER_2)));
    assertEquals(
        1,
        findAuditMessage(auditMessages,
            String.format(AuditedSecurityOperation.REVOKE_SYSTEM_PERMISSION_AUDIT_TEMPLATE, SystemPermission.ALTER_TABLE, AUDIT_USER_2)));
    assertEquals(1, findAuditMessage(auditMessages, String.format(AuditedSecurityOperation.CREATE_USER_AUDIT_TEMPLATE, "root", "")));

  }

}
