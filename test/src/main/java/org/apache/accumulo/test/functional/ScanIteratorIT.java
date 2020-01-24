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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanIteratorIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(ScanIteratorIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  private AccumuloClient accumuloClient;
  private String tableName;
  private String user;
  private boolean saslEnabled;

  @Before
  public void setup() throws Exception {
    accumuloClient = Accumulo.newClient().from(getClientProps()).build();
    tableName = getUniqueNames(1)[0];

    accumuloClient.tableOperations().create(tableName);
    ClusterUser clusterUser = getUser(0);
    user = clusterUser.getPrincipal();
    PasswordToken userToken;
    if (saslEnabled()) {
      userToken = null;
      saslEnabled = true;
    } else {
      userToken = new PasswordToken(clusterUser.getPassword());
      saslEnabled = false;
    }
    if (accumuloClient.securityOperations().listLocalUsers().contains(user)) {
      log.info("Dropping {}", user);
      accumuloClient.securityOperations().dropLocalUser(user);
    }
    accumuloClient.securityOperations().createLocalUser(user, userToken);
    accumuloClient.securityOperations().grantTablePermission(user, tableName, TablePermission.READ);
    accumuloClient.securityOperations().grantTablePermission(user, tableName,
        TablePermission.WRITE);
    accumuloClient.securityOperations().changeUserAuthorizations(user, AuthsIterator.AUTHS);
  }

  @After
  public void tearDown() throws Exception {
    if (user != null) {
      if (saslEnabled) {
        ClusterUser rootUser = getAdminUser();
        UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(),
            rootUser.getKeytab().getAbsolutePath());
      }
      accumuloClient.securityOperations().dropLocalUser(user);

      accumuloClient.close();
    }
  }

  @Test
  public void run() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (int i = 0; i < 1000; i++) {
          Mutation m = new Mutation(new Text(String.format("%06d", i)));
          m.put("cf1", "cq1", Integer.toString(1000 - i));
          m.put("cf1", "cq2", Integer.toString(i - 1000));
          bw.addMutation(m);
        }
      }

      try (Scanner scanner = c.createScanner(tableName);
          BatchScanner bscanner = c.createBatchScanner(tableName)) {

        setupIter(scanner);
        verify(scanner, 1, 999);

        bscanner.setRanges(Collections.singleton(new Range((Key) null, null)));

        setupIter(bscanner);
        verify(bscanner, 1, 999);

        ArrayList<Range> ranges = new ArrayList<>();
        ranges.add(new Range(new Text(String.format("%06d", 1))));
        ranges.add(
            new Range(new Text(String.format("%06d", 6)), new Text(String.format("%06d", 16))));
        ranges.add(new Range(new Text(String.format("%06d", 20))));
        ranges.add(new Range(new Text(String.format("%06d", 23))));
        ranges.add(
            new Range(new Text(String.format("%06d", 56)), new Text(String.format("%06d", 61))));
        ranges.add(
            new Range(new Text(String.format("%06d", 501)), new Text(String.format("%06d", 504))));
        ranges.add(
            new Range(new Text(String.format("%06d", 998)), new Text(String.format("%06d", 1000))));

        HashSet<Integer> got = new HashSet<>();
        HashSet<Integer> expected = new HashSet<>();
        for (int i : new int[] {1, 7, 9, 11, 13, 15, 23, 57, 59, 61, 501, 503, 999}) {
          expected.add(i);
        }

        bscanner.setRanges(ranges);

        for (Entry<Key,Value> entry : bscanner) {
          got.add(Integer.parseInt(entry.getKey().getRow().toString()));
        }

        System.out.println("got : " + got);

        if (!got.equals(expected)) {
          throw new Exception(got + " != " + expected);
        }
      }
    }
  }

  private void verify(Iterable<Entry<Key,Value>> scanner, int start, int finish) throws Exception {

    int expected = start;
    for (Entry<Key,Value> entry : scanner) {
      if (Integer.parseInt(entry.getKey().getRow().toString()) != expected) {
        throw new Exception("Saw unexpexted " + entry.getKey().getRow() + " " + expected);
      }

      if (entry.getKey().getColumnQualifier().toString().equals("cq2")) {
        expected += 2;
      }
    }

    if (expected != finish + 2) {
      throw new Exception("Ended at " + expected + " not " + (finish + 2));
    }
  }

  private void setupIter(ScannerBase scanner) {
    IteratorSetting dropMod =
        new IteratorSetting(50, "dropMod", "org.apache.accumulo.test.functional.DropModIter");
    dropMod.addOption("mod", "2");
    dropMod.addOption("drop", "0");
    scanner.addScanIterator(dropMod);
  }

  @Test
  public void testAuthsPresentInIteratorEnvironment() throws Exception {
    runTest(AuthsIterator.AUTHS, false);
  }

  @Test
  public void testAuthsNotPresentInIteratorEnvironment() throws Exception {
    runTest(new Authorizations("B"), true);
  }

  @Test
  public void testEmptyAuthsInIteratorEnvironment() throws Exception {
    runTest(Authorizations.EMPTY, true);
  }

  private void runTest(ScannerBase scanner, boolean shouldFail) {
    int count = 0;
    for (Map.Entry<Key,Value> entry : scanner) {
      assertEquals(shouldFail ? AuthsIterator.FAIL : AuthsIterator.SUCCESS,
          entry.getKey().getRow().toString());
      count++;
    }

    assertEquals(1, count);
  }

  private void runTest(Authorizations auths, boolean shouldFail) throws Exception {
    ClusterUser clusterUser = getUser(0);
    AccumuloClient userC =
        getCluster().createAccumuloClient(clusterUser.getPrincipal(), clusterUser.getToken());
    writeTestMutation(userC);

    IteratorSetting setting = new IteratorSetting(10, AuthsIterator.class);

    try (Scanner scanner = userC.createScanner(tableName, auths);
        BatchScanner batchScanner = userC.createBatchScanner(tableName, auths, 1)) {
      scanner.addScanIterator(setting);

      batchScanner.setRanges(Collections.singleton(new Range("1")));
      batchScanner.addScanIterator(setting);

      runTest(scanner, shouldFail);
      runTest(batchScanner, shouldFail);
    }
  }

  private void writeTestMutation(AccumuloClient userC)
      throws TableNotFoundException, MutationsRejectedException {
    try (BatchWriter batchWriter = userC.createBatchWriter(tableName)) {
      Mutation m = new Mutation("1");
      m.put("2", "3", "");
      batchWriter.addMutation(m);
      batchWriter.flush();
    }
  }
}
