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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
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
import org.apache.accumulo.test.functional.AuthsIterator;
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

  private Connector connector;
  private String tableName;
  private String user;
  private boolean saslEnabled;

  @Before
  public void setup() throws Exception {
    connector = getConnector();
    tableName = getUniqueNames(1)[0];

    connector.tableOperations().create(tableName);
    ClientConfiguration clientConfig = cluster.getClientConfig();
    ClusterUser clusterUser = getUser(0);
    user = clusterUser.getPrincipal();
    PasswordToken userToken;
    if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      userToken = null;
      saslEnabled = true;
    } else {
      userToken = new PasswordToken(clusterUser.getPassword());
      saslEnabled = false;
    }
    if (connector.securityOperations().listLocalUsers().contains(user)) {
      log.info("Dropping {}", user);
      connector.securityOperations().dropLocalUser(user);
    }
    connector.securityOperations().createLocalUser(user, userToken);
    connector.securityOperations().grantTablePermission(user, tableName, TablePermission.READ);
    connector.securityOperations().grantTablePermission(user, tableName, TablePermission.WRITE);
    connector.securityOperations().changeUserAuthorizations(user, AuthsIterator.AUTHS);
  }

  @After
  public void tearDown() throws Exception {
    if (null != user) {
      if (saslEnabled) {
        ClusterUser rootUser = getAdminUser();
        UserGroupInformation.loginUserFromKeytab(rootUser.getPrincipal(), rootUser.getKeytab().getAbsolutePath());
      }
      connector.securityOperations().dropLocalUser(user);
    }
  }

  @Test
  public void run() throws Exception {
    String tableName = getUniqueNames(1)[0];
    Connector c = getConnector();

    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());

    for (int i = 0; i < 1000; i++) {
      Mutation m = new Mutation(new Text(String.format("%06d", i)));
      m.put(new Text("cf1"), new Text("cq1"), new Value(Integer.toString(1000 - i).getBytes(UTF_8)));
      m.put(new Text("cf1"), new Text("cq2"), new Value(Integer.toString(i - 1000).getBytes(UTF_8)));

      bw.addMutation(m);
    }

    bw.close();

    Scanner scanner = c.createScanner(tableName, new Authorizations());

    setupIter(scanner);
    verify(scanner, 1, 999);

    BatchScanner bscanner = c.createBatchScanner(tableName, new Authorizations(), 3);
    bscanner.setRanges(Collections.singleton(new Range((Key) null, null)));

    setupIter(bscanner);
    verify(bscanner, 1, 999);

    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(new Text(String.format("%06d", 1))));
    ranges.add(new Range(new Text(String.format("%06d", 6)), new Text(String.format("%06d", 16))));
    ranges.add(new Range(new Text(String.format("%06d", 20))));
    ranges.add(new Range(new Text(String.format("%06d", 23))));
    ranges.add(new Range(new Text(String.format("%06d", 56)), new Text(String.format("%06d", 61))));
    ranges.add(new Range(new Text(String.format("%06d", 501)), new Text(String.format("%06d", 504))));
    ranges.add(new Range(new Text(String.format("%06d", 998)), new Text(String.format("%06d", 1000))));

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

    bscanner.close();

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

  private void setupIter(ScannerBase scanner) throws Exception {
    IteratorSetting dropMod = new IteratorSetting(50, "dropMod", "org.apache.accumulo.test.functional.DropModIter");
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

  private void runTest(ScannerBase scanner, Authorizations auths, boolean shouldFail) throws AccumuloSecurityException, AccumuloException,
      TableNotFoundException {
    int count = 0;
    for (Map.Entry<Key,Value> entry : scanner) {
      assertEquals(shouldFail ? AuthsIterator.FAIL : AuthsIterator.SUCCESS, entry.getKey().getRow().toString());
      count++;
    }

    assertEquals(1, count);
  }

  private void runTest(Authorizations auths, boolean shouldFail) throws Exception {
    ClusterUser clusterUser = getUser(0);
    Connector userC = getCluster().getConnector(clusterUser.getPrincipal(), clusterUser.getToken());
    writeTestMutation(userC);

    IteratorSetting setting = new IteratorSetting(10, AuthsIterator.class);

    Scanner scanner = userC.createScanner(tableName, auths);
    scanner.addScanIterator(setting);

    BatchScanner batchScanner = userC.createBatchScanner(tableName, auths, 1);
    batchScanner.setRanges(Collections.singleton(new Range("1")));
    batchScanner.addScanIterator(setting);

    runTest(scanner, auths, shouldFail);
    runTest(batchScanner, auths, shouldFail);

    scanner.close();
    batchScanner.close();
  }

  private void writeTestMutation(Connector userC) throws TableNotFoundException, MutationsRejectedException {
    BatchWriter batchWriter = userC.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("1");
    m.put(new Text("2"), new Text("3"), new Value("".getBytes()));
    batchWriter.addMutation(m);
    batchWriter.flush();
    batchWriter.close();

  }

}
