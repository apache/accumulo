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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.clientImpl.thrift.TVersionedProperties;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

// the shutdown test should sort last, so other tests don't break
@TestMethodOrder(MethodOrderer.MethodName.class)
public class ManagerApiIT extends SharedMiniClusterBase {

  private static Credentials rootUser;
  private static Credentials regularUser;
  private static Credentials privilegedUser;
  private static InstanceId instanceId;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
    rootUser = new Credentials(getPrincipal(), getToken());
    regularUser = new Credentials("regularUser", new PasswordToken("regularUser"));
    privilegedUser = new Credentials("privilegedUser", new PasswordToken("privilegedUser"));
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      instanceId = client.instanceOperations().getInstanceId();
      SecurityOperations rootSecOps = client.securityOperations();
      for (Credentials user : Arrays.asList(regularUser, privilegedUser)) {
        rootSecOps.createLocalUser(user.getPrincipal(), (PasswordToken) user.getToken());
      }
      rootSecOps.grantSystemPermission(privilegedUser.getPrincipal(), SystemPermission.SYSTEM);
    }
  }

  @AfterAll
  public static void teardown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private Function<Credentials,ThriftClientTypes.Exec<Void,ManagerClientService.Client>> op;

  @Test
  public void testPermissions_setManagerGoalState() throws Exception {
    // To setManagerGoalState, user needs SystemPermission.SYSTEM
    op = user -> client -> {
      client.setManagerGoalState(TraceUtil.traceInfo(), user.toThrift(instanceId),
          ManagerGoalState.NORMAL);
      return null;
    };
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

    op = user -> client -> {
      client.initiateFlush(TraceUtil.traceInfo(), user.toThrift(instanceId), tableId);
      return null;
    };
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
    op = user -> client -> {
      flushId.set(client.initiateFlush(TraceUtil.traceInfo(), user.toThrift(instanceId), tableId));
      return null;
    };
    expectPermissionSuccess(op, rootUser);
    op = user -> client -> {
      client.waitForFlush(TraceUtil.traceInfo(), user.toThrift(instanceId), tableId,
          TextUtil.getByteBuffer(new Text("myrow")), TextUtil.getByteBuffer(new Text("myrow~")),
          flushId.get(), 1);
      return null;
    };
    expectPermissionDenied(op, regularUser);
    // privileged users can grant themselves permission, but it's not default
    expectPermissionDenied(op, privilegedUser);
    expectPermissionSuccess(op, regUserWithWrite);
    expectPermissionSuccess(op, regUserWithAlter);
    // root user can because they created the table
    expectPermissionSuccess(op, rootUser);
  }

  @Test
  public void testPermissions_modifySystemProperties() throws Exception {
    // To setSystemProperty, user needs SystemPermission.SYSTEM
    String propKey = Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey();
    op = user -> client -> {
      client.modifySystemProperties(TraceUtil.traceInfo(), user.toThrift(instanceId),
          new TVersionedProperties(0, Map.of(propKey, "10000")));
      return null;
    };
    expectPermissionDenied(op, regularUser);
    expectPermissionSuccess(op, rootUser);
    op = user -> client -> {
      client.modifySystemProperties(TraceUtil.traceInfo(), user.toThrift(instanceId),
          new TVersionedProperties(1, Map.of(propKey, "10000")));
      return null;
    };
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
    op = user -> client -> {
      client.removeSystemProperty(TraceUtil.traceInfo(), user.toThrift(instanceId), propKey1);
      return null;
    };
    expectPermissionDenied(op, regularUser);
    expectPermissionSuccess(op, rootUser);
    op = user -> client -> {
      client.removeSystemProperty(TraceUtil.traceInfo(), user.toThrift(instanceId), propKey2);
      return null;
    };
    expectPermissionSuccess(op, privilegedUser);
  }

  @Test
  public void testPermissions_setSystemProperty() throws Exception {
    // To setSystemProperty, user needs SystemPermission.SYSTEM
    String propKey = Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey();
    op = user -> client -> {
      client.setSystemProperty(TraceUtil.traceInfo(), user.toThrift(instanceId), propKey, "10000");
      return null;
    };
    expectPermissionDenied(op, regularUser);
    expectPermissionSuccess(op, rootUser);
    expectPermissionSuccess(op, privilegedUser);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.instanceOperations().removeProperty(propKey); // clean up property
    }
  }

  @Test
  public void testPermissions_shutdownTabletServer() throws Exception {
    // To shutdownTabletServer, user needs SystemPermission.SYSTEM
    // this server won't exist, so shutting it down is a NOOP on success
    String fakeHostAndPort = getUniqueNames(1)[0] + ":0";
    op = user -> client -> {
      client.shutdownTabletServer(TraceUtil.traceInfo(), user.toThrift(instanceId), fakeHostAndPort,
          false);
      return null;
    };
    expectPermissionDenied(op, regularUser);
    expectPermissionSuccess(op, rootUser);
    expectPermissionSuccess(op, privilegedUser);
  }

  @Test
  public void shutdownTabletServer() throws Exception {
    op = user -> client -> {
      client.shutdownTabletServer(TraceUtil.traceInfo(), user.toThrift(instanceId),
          "fakeTabletServer:9997", true);
      return null;
    };
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps())
        .as(rootUser.getPrincipal(), rootUser.getToken()).build()) {
      ClientContext context = (ClientContext) client;
      ThriftClientTypes.MANAGER.execute(context, op.apply(rootUser), ResourceGroupId.DEFAULT);
    }
  }

  // this test should go last, because it shuts things down;
  // see the junit annotation to control test ordering at the top of this class
  @Test
  public void z99_testPermissions_shutdown() throws Exception {
    // grab connections before shutting down
    var rootUserBuilder = Accumulo.newClient().from(getClientProps()).as(rootUser.getPrincipal(),
        rootUser.getToken());
    var privUserBuilder = Accumulo.newClient().from(getClientProps())
        .as(privilegedUser.getPrincipal(), privilegedUser.getToken());
    try (var rootClient = rootUserBuilder.build(); var privClient = privUserBuilder.build()) {
      // To shutdown, user needs SystemPermission.SYSTEM
      op = user -> client -> {
        client.shutdown(TraceUtil.traceInfo(), user.toThrift(instanceId), false);
        return null;
      };
      expectPermissionDenied(op, regularUser);
      expectPermissionSuccess(op.apply(rootUser), (ClientContext) rootClient);

      // make sure it's stopped, then start it again to test with the privileged user
      getCluster().stop();
      getCluster().start();

      // make sure regular user is still denied after restart
      expectPermissionDenied(op, regularUser);
      expectPermissionSuccess(op.apply(privilegedUser), (ClientContext) privClient);
    }
  }

  private static void expectPermissionSuccess(
      Function<Credentials,ThriftClientTypes.Exec<Void,ManagerClientService.Client>> op,
      Credentials user) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps())
        .as(user.getPrincipal(), user.getToken()).build()) {
      ThriftClientTypes.MANAGER.execute((ClientContext) client, op.apply(user),
          ResourceGroupId.DEFAULT);
    }
  }

  private static void expectPermissionSuccess(
      ThriftClientTypes.Exec<Void,ManagerClientService.Client> op, ClientContext context)
      throws Exception {
    ThriftClientTypes.MANAGER.execute(context, op, ResourceGroupId.DEFAULT);
  }

  private static void expectPermissionDenied(
      Function<Credentials,ThriftClientTypes.Exec<Void,ManagerClientService.Client>> op,
      Credentials user) {
    var e = assertThrows(AccumuloSecurityException.class, () -> expectPermissionSuccess(op, user));
    assertSame(SecurityErrorCode.PERMISSION_DENIED, e.getSecurityErrorCode());
  }

}
