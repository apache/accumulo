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
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
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
    // need to pretend to be a server, so we can bypass all of
    // the singleton resource management in this test
    SingletonManager.setMode(Mode.SERVER);
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

  private ThriftClientTypes.Exec<Void,ManagerClientService.Client> op;

  @Test
  public void testPermissions_setManagerGoalState() throws Exception {
    // To setManagerGoalState, user needs SystemPermission.SYSTEM
    op = client -> {
      client.setManagerGoalState(TraceUtil.traceInfo(), regularUser.toThrift(instanceId),
          ManagerGoalState.NORMAL);
      return null;
    };
    expectPermissionDenied(op, regularUser);
    op = client -> {
      client.setManagerGoalState(TraceUtil.traceInfo(), rootUser.toThrift(instanceId),
          ManagerGoalState.NORMAL);
      return null;
    };
    expectPermissionSuccess(op, rootUser);
    op = client -> {
      client.setManagerGoalState(TraceUtil.traceInfo(), privilegedUser.toThrift(instanceId),
          ManagerGoalState.NORMAL);
      return null;
    };
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

    op = client -> {
      client.initiateFlush(TraceUtil.traceInfo(), regularUser.toThrift(instanceId), tableId);
      return null;
    };
    expectPermissionDenied(op, regularUser);
    // privileged users can grant themselves permission, but it's not default
    op = client -> {
      client.initiateFlush(TraceUtil.traceInfo(), privilegedUser.toThrift(instanceId), tableId);
      return null;
    };
    expectPermissionDenied(op, privilegedUser);
    op = client -> {
      client.initiateFlush(TraceUtil.traceInfo(), regUserWithWrite.toThrift(instanceId), tableId);
      return null;
    };
    expectPermissionSuccess(op, regUserWithWrite);
    op = client -> {
      client.initiateFlush(TraceUtil.traceInfo(), regUserWithAlter.toThrift(instanceId), tableId);
      return null;
    };
    expectPermissionSuccess(op, regUserWithAlter);
    // root user can because they created the table
    op = client -> {
      client.initiateFlush(TraceUtil.traceInfo(), rootUser.toThrift(instanceId), tableId);
      return null;
    };
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
    op = client -> {
      flushId
          .set(client.initiateFlush(TraceUtil.traceInfo(), rootUser.toThrift(instanceId), tableId));
      return null;
    };
    expectPermissionSuccess(op, rootUser);
    op = client -> {
      client.waitForFlush(TraceUtil.traceInfo(), regularUser.toThrift(instanceId), tableId,
          TextUtil.getByteBuffer(new Text("myrow")), TextUtil.getByteBuffer(new Text("myrow~")),
          flushId.get(), 1);
      return null;
    };
    expectPermissionDenied(op, regularUser);
    // privileged users can grant themselves permission, but it's not default
    op = client -> {
      client.waitForFlush(TraceUtil.traceInfo(), privilegedUser.toThrift(instanceId), tableId,
          TextUtil.getByteBuffer(new Text("myrow")), TextUtil.getByteBuffer(new Text("myrow~")),
          flushId.get(), 1);
      return null;
    };
    expectPermissionDenied(op, privilegedUser);
    op = client -> {
      client.waitForFlush(TraceUtil.traceInfo(), regUserWithWrite.toThrift(instanceId), tableId,
          TextUtil.getByteBuffer(new Text("myrow")), TextUtil.getByteBuffer(new Text("myrow~")),
          flushId.get(), 1);
      return null;
    };
    expectPermissionSuccess(op, regUserWithWrite);
    op = client -> {
      client.waitForFlush(TraceUtil.traceInfo(), regUserWithAlter.toThrift(instanceId), tableId,
          TextUtil.getByteBuffer(new Text("myrow")), TextUtil.getByteBuffer(new Text("myrow~")),
          flushId.get(), 1);
      return null;
    };
    expectPermissionSuccess(op, regUserWithAlter);
    // root user can because they created the table
    op = client -> {
      client.waitForFlush(TraceUtil.traceInfo(), rootUser.toThrift(instanceId), tableId,
          TextUtil.getByteBuffer(new Text("myrow")), TextUtil.getByteBuffer(new Text("myrow~")),
          flushId.get(), 1);
      return null;
    };
    expectPermissionSuccess(op, rootUser);
  }

  @Test
  public void testPermissions_modifySystemProperties() throws Exception {
    // To setSystemProperty, user needs SystemPermission.SYSTEM
    String propKey = Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey();
    op = client -> {
      client.modifySystemProperties(TraceUtil.traceInfo(), regularUser.toThrift(instanceId),
          new TVersionedProperties(0, Map.of(propKey, "10000")));
      return null;
    };
    expectPermissionDenied(op, regularUser);
    op = client -> {
      client.modifySystemProperties(TraceUtil.traceInfo(), rootUser.toThrift(instanceId),
          new TVersionedProperties(0, Map.of(propKey, "10000")));
      return null;
    };
    expectPermissionSuccess(op, rootUser);
    op = client -> {
      client.modifySystemProperties(TraceUtil.traceInfo(), privilegedUser.toThrift(instanceId),
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
    op = client -> {
      client.removeSystemProperty(TraceUtil.traceInfo(), regularUser.toThrift(instanceId),
          propKey1);
      return null;
    };
    expectPermissionDenied(op, regularUser);
    op = client -> {
      client.removeSystemProperty(TraceUtil.traceInfo(), rootUser.toThrift(instanceId), propKey1);
      return null;
    };
    expectPermissionSuccess(op, rootUser);
    op = client -> {
      client.removeSystemProperty(TraceUtil.traceInfo(), privilegedUser.toThrift(instanceId),
          propKey2);
      return null;
    };
    expectPermissionSuccess(op, privilegedUser);
  }

  @Test
  public void testPermissions_setSystemProperty() throws Exception {
    // To setSystemProperty, user needs SystemPermission.SYSTEM
    String propKey = Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey();
    op = client -> {
      client.setSystemProperty(TraceUtil.traceInfo(), regularUser.toThrift(instanceId), propKey,
          "10000");
      return null;
    };
    expectPermissionDenied(op, regularUser);
    op = client -> {
      client.setSystemProperty(TraceUtil.traceInfo(), rootUser.toThrift(instanceId), propKey,
          "10000");
      return null;
    };
    expectPermissionSuccess(op, rootUser);
    op = client -> {
      client.setSystemProperty(TraceUtil.traceInfo(), privilegedUser.toThrift(instanceId), propKey,
          "10000");
      return null;
    };
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
    op = client -> {
      client.shutdownTabletServer(TraceUtil.traceInfo(), regularUser.toThrift(instanceId),
          fakeHostAndPort, false);
      return null;
    };
    expectPermissionDenied(op, regularUser);
    op = client -> {
      client.shutdownTabletServer(TraceUtil.traceInfo(), rootUser.toThrift(instanceId),
          fakeHostAndPort, false);
      return null;
    };
    expectPermissionSuccess(op, rootUser);
    op = client -> {
      client.shutdownTabletServer(TraceUtil.traceInfo(), privilegedUser.toThrift(instanceId),
          fakeHostAndPort, false);
      return null;
    };
    expectPermissionSuccess(op, privilegedUser);
  }

  @Test
  public void shutdownTabletServer() throws Exception {
    op = client -> {
      client.shutdownTabletServer(TraceUtil.traceInfo(), rootUser.toThrift(instanceId),
          "fakeTabletServer:9997", true);
      return null;
    };
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps())
        .as(rootUser.getPrincipal(), rootUser.getToken()).build()) {
      ClientContext context = (ClientContext) client;
      ThriftClientTypes.MANAGER.execute(context, op);
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
      op = client -> {
        client.shutdown(TraceUtil.traceInfo(), regularUser.toThrift(instanceId), false);
        return null;
      };
      expectPermissionDenied(op, regularUser);
      // We should be able to do both of the following RPC calls before it actually shuts down
      op = client -> {
        client.shutdown(TraceUtil.traceInfo(), rootUser.toThrift(instanceId), false);
        return null;
      };
      expectPermissionSuccess(op, (ClientContext) rootClient);
      op = client -> {
        client.shutdown(TraceUtil.traceInfo(), privilegedUser.toThrift(instanceId), false);
        return null;
      };
      expectPermissionSuccess(op, (ClientContext) privClient);
    }
  }

  private static void expectPermissionSuccess(
      ThriftClientTypes.Exec<Void,ManagerClientService.Client> op, Credentials user)
      throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps())
        .as(user.getPrincipal(), user.getToken()).build()) {
      ClientContext context = (ClientContext) client;
      ThriftClientTypes.MANAGER.execute(context, op);
    }
  }

  private static void expectPermissionSuccess(
      ThriftClientTypes.Exec<Void,ManagerClientService.Client> op, ClientContext context)
      throws Exception {
    ThriftClientTypes.MANAGER.execute(context, op);
  }

  private static void expectPermissionDenied(
      ThriftClientTypes.Exec<Void,ManagerClientService.Client> op, Credentials user) {
    AccumuloSecurityException e =
        assertThrows(AccumuloSecurityException.class, () -> expectPermissionSuccess(op, user));
    assertSame(SecurityErrorCode.PERMISSION_DENIED, e.getSecurityErrorCode());
  }

}
