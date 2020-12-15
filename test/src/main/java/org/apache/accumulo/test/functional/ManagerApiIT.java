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

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MiniClusterOnlyTests.class)
public class ManagerApiIT extends AccumuloClusterHarness {

  private static final Logger LOG = LoggerFactory.getLogger(ManagerApiIT.class);

  private interface MasterApiMethodTest {
    public void test() throws Exception;
  }

  private class Flush implements MasterApiMethodTest {
    @Override
    public void test() throws Exception {
      MasterClient.executeVoid(getContext(), new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          String tableId =
              getContext().getConnector().tableOperations().tableIdMap().get("ns1.NO_SUCH_TABLE");
          LOG.info("Initiating Flush");
          long flushID = client.initiateFlush(null, getContext().rpcCreds(), tableId);
          client.waitForFlush(Tracer.traceInfo(), getContext().rpcCreds(), tableId,
              TextUtil.getByteBuffer(new Text("myrow")), TextUtil.getByteBuffer(new Text("myrow~")),
              flushID, 1);
        }
      });
    }
  }

  private class ShutDown implements MasterApiMethodTest {
    public void test() throws Exception {
      MasterClient.executeVoid(getContext(), new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          LOG.info("Sending ShutDown command via MasterClientService");
          client.shutdown(null, getContext().rpcCreds(), false);
        }
      });
    }
  }

  private class ShutDownTServer implements MasterApiMethodTest {
    public void test() throws Exception {
      MasterClient.executeVoid(getContext(), new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          LOG.info("Sending shutdown Tserver command to {} via MasterClientService",
              "NO_SUCH_TSERVER");
          client.shutdownTabletServer(null, getContext().rpcCreds(), "NO_SUCH_TSERVER", false);
        }
      });
    }
  }

  private class SetMasterGoalState implements MasterApiMethodTest {
    public void test() throws Exception {
      MasterClient.executeVoid(getContext(), new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          LOG.info("Setting MasterGoalState to {} via MasterClientService",
              MasterGoalState.CLEAN_STOP);
          client.setMasterGoalState(null, getContext().rpcCreds(), MasterGoalState.CLEAN_STOP);
        }
      });
    }
  }

  private class SetSystemProperty implements MasterApiMethodTest {
    public void test() throws Exception {
      MasterClient.executeVoid(getContext(), new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          String prop = Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey();
          String value = "10000";
          LOG.info("Setting system property {} to {} via MasterClientService", prop, value);
          client.setSystemProperty(null, getContext().rpcCreds(), prop, value);
        }
      });
    }
  }

  private class RemoveSystemProperty implements MasterApiMethodTest {
    public void test() throws Exception {
      MasterClient.executeVoid(getContext(), new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          String prop = Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey();
          LOG.info("Removing system property {} via MasterClientService", prop);
          client.removeSystemProperty(null, getContext().rpcCreds(), prop);
        }
      });
    }
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  private ClientContext getContext() {
    return new ClientContext(this.getConnector().getInstance(), user,
        getClusterConfiguration().getClientConf());
  }

  private void runTest(MasterApiMethodTest test, boolean expectException,
      boolean expectPermissionDenied) throws Exception {
    try {
      test.test();
    } catch (Exception ex) {
      if (!expectException) {
        throw ex;
      }
      if (ex instanceof AccumuloSecurityException && ((AccumuloSecurityException) ex)
          .getSecurityErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
        if (!expectPermissionDenied) {
          throw ex;
        }
      }
    }
  }

  private volatile Credentials user = null;

  @Test
  public void testManagerApi() throws Exception {
    // Set user to ADMIN user
    Credentials admin = new Credentials(getAdminPrincipal(), getAdminToken());
    Credentials user1 = new Credentials("user1", new PasswordToken("user1"));
    Credentials user2 = new Credentials("user2", new PasswordToken("user2"));

    user = admin;
    getContext().getConnector().securityOperations().createLocalUser(user1.getPrincipal(),
        (PasswordToken) user1.getToken());
    getContext().getConnector().securityOperations().createLocalUser(user2.getPrincipal(),
        (PasswordToken) user2.getToken());
    getContext().getConnector().securityOperations().grantSystemPermission(user1.getPrincipal(),
        SystemPermission.CREATE_NAMESPACE);
    getContext().getConnector().securityOperations().grantSystemPermission(user2.getPrincipal(),
        SystemPermission.SYSTEM);
    // for Flush test. Create namespace and table and give user2 write permissions.
    getContext().getConnector().namespaceOperations().create("ns1");
    getContext().getConnector().tableOperations().create("ns1.NO_SUCH_TABLE");
    getContext().getConnector().securityOperations().grantTablePermission(user2.getPrincipal(),
        "ns1.NO_SUCH_TABLE", TablePermission.WRITE);

    // To setMasterGoalState, user needs System.system
    user = admin; // admin has System.system
    runTest(new SetMasterGoalState(), false, false);
    user = user1; // user1 doesn't have System.system
    runTest(new SetMasterGoalState(), true, true);
    user = user2; // user2 has System.system
    runTest(new SetMasterGoalState(), false, false);

    // To Flush, user needs WRITE or ALTER TABLE
    user = admin; // admin created the table
    runTest(new Flush(), false, false);
    user = user1; // user1 doesn't have Write permissions
    runTest(new Flush(), true, true);
    user = user2; // user2 has write permissions
    runTest(new Flush(), false, false);

    // To set system property, user needs System.system
    user = admin; // admin has System.system
    runTest(new SetSystemProperty(), false, false);
    user = user1; // user1 doesn't have System.system
    runTest(new SetSystemProperty(), true, true);
    user = user2; // user2 has System.system
    runTest(new SetSystemProperty(), false, false);

    // To remove system property, user needs System.system
    user = admin; // admin has System.system
    runTest(new RemoveSystemProperty(), false, false);
    user = user1; // user1 doesn't have System.system
    runTest(new RemoveSystemProperty(), true, true);
    user = user2; // user2 has System.system
    runTest(new RemoveSystemProperty(), true, false);

    // To shutdown Tserver, user needs System.system
    user = admin; // admin has System.system
    runTest(new ShutDownTServer(), true, false);
    user = user1; // user1 doesn't have System.system
    runTest(new ShutDownTServer(), true, true);
    user = user2; // user2 has System.system
    runTest(new ShutDownTServer(), true, false);

    // To shutdown, user needs System.system
    user = admin; // admin has System.system
    runTest(new ShutDown(), false, false);
    user = user1; // user1 doesn't have System.system
    runTest(new ShutDown(), true, true);
    user = user2; // user2 has System.system
    runTest(new ShutDown(), false, false);

  }

}
