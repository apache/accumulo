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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientExec;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.clientImpl.MasterClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

// the shutdown test should sort last, so other tests don't break
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ManagerApiIT extends SharedMiniClusterBase {

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  private static Credentials rootUser;
  private static Credentials regularUser;
  private static Credentials privilegedUser;

  @BeforeClass
  public static void setup() throws Exception {
    // need to pretend to be a server, so we can bypass all of
    // the singleton resource management in this test
    SingletonManager.setMode(Mode.SERVER);
    SharedMiniClusterBase.startMiniCluster();
    rootUser = new Credentials(getPrincipal(), getToken());
    regularUser = new Credentials("regularUser", new PasswordToken("regularUser"));
    privilegedUser = new Credentials("privilegedUser", new PasswordToken("privilegedUser"));
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      SecurityOperations rootSecOps = client.securityOperations();
      for (Credentials user : Arrays.asList(regularUser, privilegedUser))
        rootSecOps.createLocalUser(user.getPrincipal(), (PasswordToken) user.getToken());
      rootSecOps.grantSystemPermission(privilegedUser.getPrincipal(), SystemPermission.SYSTEM);
    }
  }

  @AfterClass
  public static void teardown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private Function<TCredentials,ClientExec<MasterClientService.Client>> op;

  @Test
  public void testPermissions_setMasterGoalState() throws Exception {
    // To setMasterGoalState, user needs SystemPermission.SYSTEM
    op = user -> client -> client.setMasterGoalState(null, user, MasterGoalState.NORMAL);
    expectPermissionDenied(op, regularUser);
    expectPermissionSuccess(op, rootUser);
    expectPermissionSuccess(op, privilegedUser);
  }

  @Test
  public void testPermissions_initiateFlush() throws Exception {
    // To initiateFlush, user needs TablePermission.WRITE or TablePermission.ALTER_TABLE
    String[] uniqNames = getUniqueNames(3);
    String tableName = uniqNames[0];
    Credentials regUserWithWrite = new Credentials(uniqNames[1], new PasswordToken(uniqNames[1]));
    Credentials regUserWithAlter = new Credentials(uniqNames[2], new PasswordToken(uniqNames[2]));
    String tableId;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      SecurityOperations rootSecOps = client.securityOperations();
      rootSecOps.createLocalUser(regUserWithWrite.getPrincipal(),
          (PasswordToken) regUserWithWrite.getToken());
      rootSecOps.createLocalUser(regUserWithAlter.getPrincipal(),
          (PasswordToken) regUserWithAlter.getToken());
      client.tableOperations().create(tableName);
      rootSecOps.grantTablePermission(regUserWithWrite.getPrincipal(), tableName,
          TablePermission.WRITE);
      rootSecOps.grantTablePermission(regUserWithAlter.getPrincipal(), tableName,
          TablePermission.ALTER_TABLE);
      tableId = client.tableOperations().tableIdMap().get(tableName);
    }
    op = user -> client -> client.initiateFlush(null, user, tableId);
    expectPermissionDenied(op, regularUser);
    // privileged users can grant themselves permission, but it's not default
    expectPermissionDenied(op, privilegedUser);
    expectPermissionSuccess(op, regUserWithWrite);
    expectPermissionSuccess(op, regUserWithAlter);
    // root user can because they created the table
    expectPermissionSuccess(op, rootUser);
  }

  @Test
  public void testPermissions_waitForFlush() throws Exception {
    // To waitForFlush, user needs TablePermission.WRITE or TablePermission.ALTER_TABLE
    String[] uniqNames = getUniqueNames(3);
    String tableName = uniqNames[0];
    Credentials regUserWithWrite = new Credentials(uniqNames[1], new PasswordToken(uniqNames[1]));
    Credentials regUserWithAlter = new Credentials(uniqNames[2], new PasswordToken(uniqNames[2]));
    String tableId;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      SecurityOperations rootSecOps = client.securityOperations();
      rootSecOps.createLocalUser(regUserWithWrite.getPrincipal(),
          (PasswordToken) regUserWithWrite.getToken());
      rootSecOps.createLocalUser(regUserWithAlter.getPrincipal(),
          (PasswordToken) regUserWithAlter.getToken());
      client.tableOperations().create(tableName);
      rootSecOps.grantTablePermission(regUserWithWrite.getPrincipal(), tableName,
          TablePermission.WRITE);
      rootSecOps.grantTablePermission(regUserWithAlter.getPrincipal(), tableName,
          TablePermission.ALTER_TABLE);
      tableId = client.tableOperations().tableIdMap().get(tableName);
    }
    AtomicLong flushId = new AtomicLong();
    // initiateFlush as the root user to get the flushId, then test waitForFlush with other users
    op = user -> client -> flushId.set(client.initiateFlush(null, user, tableId));
    expectPermissionSuccess(op, rootUser);
    op = user -> client -> client.waitForFlush(null, user, tableId,
        TextUtil.getByteBuffer(new Text("myrow")), TextUtil.getByteBuffer(new Text("myrow~")),
        flushId.get(), 1);
    expectPermissionDenied(op, regularUser);
    // privileged users can grant themselves permission, but it's not default
    expectPermissionDenied(op, privilegedUser);
    expectPermissionSuccess(op, regUserWithWrite);
    expectPermissionSuccess(op, regUserWithAlter);
    // root user can because they created the table
    expectPermissionSuccess(op, rootUser);
  }

  @Test
  public void testPermissions_setSystemProperty() throws Exception {
    // To setSystemProperty, user needs SystemPermission.SYSTEM
    String propKey = Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey();
    op = user -> client -> client.setSystemProperty(null, user, propKey, "10000");
    expectPermissionDenied(op, regularUser);
    expectPermissionSuccess(op, rootUser);
    expectPermissionSuccess(op, privilegedUser);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.instanceOperations().removeProperty(propKey); // clean up property
    }
  }

  @Test
  public void testPermissions_removeSystemProperty() throws Exception {
    // To removeSystemProperty, user needs SystemPermission.SYSTEM
    String propKey1 = Property.GC_CYCLE_DELAY.getKey();
    String propKey2 = Property.GC_CYCLE_START.getKey();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.instanceOperations().setProperty(propKey1, "10000"); // ensure it exists
      client.instanceOperations().setProperty(propKey2, "10000"); // ensure it exists
    }
    op = user -> client -> client.removeSystemProperty(null, user, propKey1);
    expectPermissionDenied(op, regularUser);
    expectPermissionSuccess(op, rootUser);
    op = user -> client -> client.removeSystemProperty(null, user, propKey2);
    expectPermissionSuccess(op, privilegedUser);
  }

  @Test
  public void testPermissions_shutdownTabletServer() throws Exception {
    // To shutdownTabletServer, user needs SystemPermission.SYSTEM
    // this server won't exist, so shutting it down is a NOOP on success
    String fakeHostAndPort = getUniqueNames(1)[0] + ":0";
    op = user -> client -> client.shutdownTabletServer(null, user, fakeHostAndPort, false);
    expectPermissionDenied(op, regularUser);
    expectPermissionSuccess(op, rootUser);
    expectPermissionSuccess(op, privilegedUser);
  }

  // this test should go last, because it shuts things down;
  // see the junit annotation to control test ordering at the top of this class
  @Test
  public void z99_testPermissions_shutdown() throws Exception {
    // To shutdown, user needs SystemPermission.SYSTEM
    op = user -> client -> client.shutdown(null, user, false);
    expectPermissionDenied(op, regularUser);
    // We should be able to do both of the following RPC calls before it actually shuts down
    expectPermissionSuccess(op, rootUser);
    expectPermissionSuccess(op, privilegedUser);
  }

  private static void expectPermissionSuccess(
      Function<TCredentials,ClientExec<MasterClientService.Client>> op, Credentials user)
      throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps())
        .as(user.getPrincipal(), user.getToken()).build()) {
      ClientContext context = (ClientContext) client;
      MasterClient.executeVoid(context, op.apply(context.rpcCreds()));
    }
  }

  private static void expectPermissionDenied(
      Function<TCredentials,ClientExec<MasterClientService.Client>> op, Credentials user)
      throws Exception {
    AccumuloSecurityException e =
        assertThrows(AccumuloSecurityException.class, () -> expectPermissionSuccess(op, user));
    assertSame(SecurityErrorCode.PERMISSION_DENIED, e.getSecurityErrorCode());
  }

}
