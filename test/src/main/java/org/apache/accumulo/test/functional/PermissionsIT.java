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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This test verifies the default permissions so a clean instance must be used. A shared instance might
// not be representative of a fresh installation.
@Category(MiniClusterOnlyTests.class)
public class PermissionsIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(PermissionsIT.class);

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Before
  public void limitToMini() throws Exception {
    Assume.assumeTrue(ClusterType.MINI == getClusterType());
    Connector c = getConnector();
    Set<String> users = c.securityOperations().listLocalUsers();
    ClusterUser user = getUser(0);
    if (users.contains(user.getPrincipal())) {
      c.securityOperations().dropLocalUser(user.getPrincipal());
    }
  }

  private void loginAs(ClusterUser user) throws IOException {
    // Force a re-login as the provided user
    user.getToken();
  }

  @Test
  public void systemPermissionsTest() throws Exception {
    ClusterUser testUser = getUser(0), rootUser = getAdminUser();

    // verify that the test is being run by root
    Connector c = getConnector();
    verifyHasOnlyTheseSystemPermissions(c, c.whoami(), SystemPermission.values());

    // create the test user
    String principal = testUser.getPrincipal();
    AuthenticationToken token = testUser.getToken();
    PasswordToken passwordToken = null;
    if (token instanceof PasswordToken) {
      passwordToken = (PasswordToken) token;
    }
    loginAs(rootUser);
    c.securityOperations().createLocalUser(principal, passwordToken);
    loginAs(testUser);
    Connector test_user_conn = c.getInstance().getConnector(principal, token);
    loginAs(rootUser);
    verifyHasNoSystemPermissions(c, principal, SystemPermission.values());

    // test each permission
    for (SystemPermission perm : SystemPermission.values()) {
      log.debug("Verifying the {} permission", perm);

      // test permission before and after granting it
      String tableNamePrefix = getUniqueNames(1)[0];
      testMissingSystemPermission(tableNamePrefix, c, rootUser, test_user_conn, testUser, perm);
      loginAs(rootUser);
      c.securityOperations().grantSystemPermission(principal, perm);
      verifyHasOnlyTheseSystemPermissions(c, principal, perm);
      testGrantedSystemPermission(tableNamePrefix, c, rootUser, test_user_conn, testUser, perm);
      loginAs(rootUser);
      c.securityOperations().revokeSystemPermission(principal, perm);
      verifyHasNoSystemPermissions(c, principal, perm);
    }
  }

  static Map<String,String> map(Iterable<Entry<String,String>> i) {
    Map<String,String> result = new HashMap<>();
    for (Entry<String,String> e : i) {
      result.put(e.getKey(), e.getValue());
    }
    return result;
  }

  private void testMissingSystemPermission(String tableNamePrefix, Connector root_conn, ClusterUser rootUser, Connector test_user_conn, ClusterUser testUser,
      SystemPermission perm) throws Exception {
    String tableName, user, password = "password", namespace;
    boolean passwordBased = testUser.getPassword() != null;
    log.debug("Confirming that the lack of the {} permission properly restricts the user", perm);

    // test permission prior to granting it
    switch (perm) {
      case CREATE_TABLE:
        tableName = tableNamePrefix + "__CREATE_TABLE_WITHOUT_PERM_TEST__";
        try {
          loginAs(testUser);
          test_user_conn.tableOperations().create(tableName);
          throw new IllegalStateException("Should NOT be able to create a table");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || root_conn.tableOperations().list().contains(tableName))
            throw e;
        }
        break;
      case DROP_TABLE:
        tableName = tableNamePrefix + "__DROP_TABLE_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_conn.tableOperations().create(tableName);
        try {
          loginAs(testUser);
          test_user_conn.tableOperations().delete(tableName);
          throw new IllegalStateException("Should NOT be able to delete a table");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || !root_conn.tableOperations().list().contains(tableName))
            throw e;
        }
        break;
      case ALTER_TABLE:
        tableName = tableNamePrefix + "__ALTER_TABLE_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_conn.tableOperations().create(tableName);
        try {
          loginAs(testUser);
          test_user_conn.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
          throw new IllegalStateException("Should NOT be able to set a table property");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || map(root_conn.tableOperations().getProperties(tableName)).get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
            throw e;
        }
        loginAs(rootUser);
        root_conn.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
        try {
          loginAs(testUser);
          test_user_conn.tableOperations().removeProperty(tableName, Property.TABLE_BLOOM_ERRORRATE.getKey());
          throw new IllegalStateException("Should NOT be able to remove a table property");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !map(root_conn.tableOperations().getProperties(tableName)).get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
            throw e;
        }
        String table2 = tableName + "2";
        try {
          loginAs(testUser);
          test_user_conn.tableOperations().rename(tableName, table2);
          throw new IllegalStateException("Should NOT be able to rename a table");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || !root_conn.tableOperations().list().contains(tableName)
              || root_conn.tableOperations().list().contains(table2))
            throw e;
        }
        break;
      case CREATE_USER:
        user = "__CREATE_USER_WITHOUT_PERM_TEST__";
        try {
          loginAs(testUser);
          test_user_conn.securityOperations().createLocalUser(user, (passwordBased ? new PasswordToken(password) : null));
          throw new IllegalStateException("Should NOT be able to create a user");
        } catch (AccumuloSecurityException e) {
          AuthenticationToken userToken = testUser.getToken();
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || (userToken instanceof PasswordToken && root_conn.securityOperations().authenticateUser(user, userToken)))
            throw e;
        }
        break;
      case DROP_USER:
        user = "__DROP_USER_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_conn.securityOperations().createLocalUser(user, (passwordBased ? new PasswordToken(password) : null));
        try {
          loginAs(testUser);
          test_user_conn.securityOperations().dropLocalUser(user);
          throw new IllegalStateException("Should NOT be able to delete a user");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || !root_conn.securityOperations().listLocalUsers().contains(user)) {
            log.info("Failed to authenticate as {}", user);
            throw e;
          }
        }
        break;
      case ALTER_USER:
        user = "__ALTER_USER_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_conn.securityOperations().createLocalUser(user, (passwordBased ? new PasswordToken(password) : null));
        try {
          loginAs(testUser);
          test_user_conn.securityOperations().changeUserAuthorizations(user, new Authorizations("A", "B"));
          throw new IllegalStateException("Should NOT be able to alter a user");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || !root_conn.securityOperations().getUserAuthorizations(user).isEmpty())
            throw e;
        }
        break;
      case SYSTEM:
        // test for system permission would go here
        break;
      case CREATE_NAMESPACE:
        namespace = "__CREATE_NAMESPACE_WITHOUT_PERM_TEST__";
        try {
          loginAs(testUser);
          test_user_conn.namespaceOperations().create(namespace);
          throw new IllegalStateException("Should NOT be able to create a namespace");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || root_conn.namespaceOperations().list().contains(namespace))
            throw e;
        }
        break;
      case DROP_NAMESPACE:
        namespace = "__DROP_NAMESPACE_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_conn.namespaceOperations().create(namespace);
        try {
          loginAs(testUser);
          test_user_conn.namespaceOperations().delete(namespace);
          throw new IllegalStateException("Should NOT be able to delete a namespace");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || !root_conn.namespaceOperations().list().contains(namespace))
            throw e;
        }
        break;
      case ALTER_NAMESPACE:
        namespace = "__ALTER_NAMESPACE_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_conn.namespaceOperations().create(namespace);
        try {
          loginAs(testUser);
          test_user_conn.namespaceOperations().setProperty(namespace, Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
          throw new IllegalStateException("Should NOT be able to set a namespace property");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || map(root_conn.namespaceOperations().getProperties(namespace)).get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
            throw e;
        }
        loginAs(rootUser);
        root_conn.namespaceOperations().setProperty(namespace, Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
        try {
          loginAs(testUser);
          test_user_conn.namespaceOperations().removeProperty(namespace, Property.TABLE_BLOOM_ERRORRATE.getKey());
          throw new IllegalStateException("Should NOT be able to remove a namespace property");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !map(root_conn.namespaceOperations().getProperties(namespace)).get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
            throw e;
        }
        String namespace2 = namespace + "2";
        try {
          loginAs(testUser);
          test_user_conn.namespaceOperations().rename(namespace, namespace2);
          throw new IllegalStateException("Should NOT be able to rename a namespace");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || !root_conn.namespaceOperations().list().contains(namespace)
              || root_conn.namespaceOperations().list().contains(namespace2))
            throw e;
        }
        break;
      case OBTAIN_DELEGATION_TOKEN:
        ClientConfiguration clientConf = cluster.getClientConfig();
        if (clientConf.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
          // TODO Try to obtain a delegation token without the permission
        }
        break;
      case GRANT:
        loginAs(testUser);
        try {
          test_user_conn.securityOperations().grantSystemPermission(testUser.getPrincipal(), SystemPermission.GRANT);
          throw new IllegalStateException("Should NOT be able to grant System.GRANT to yourself");
        } catch (AccumuloSecurityException e) {
          // Expected
          loginAs(rootUser);
          assertFalse(root_conn.securityOperations().hasSystemPermission(testUser.getPrincipal(), SystemPermission.GRANT));
        }
        break;
      default:
        throw new IllegalArgumentException("Unrecognized System Permission: " + perm);
    }
  }

  private void testGrantedSystemPermission(String tableNamePrefix, Connector root_conn, ClusterUser rootUser, Connector test_user_conn, ClusterUser testUser,
      SystemPermission perm) throws Exception {
    String tableName, user, password = "password", namespace;
    boolean passwordBased = testUser.getPassword() != null;
    log.debug("Confirming that the presence of the {} permission properly permits the user", perm);

    // test permission after granting it
    switch (perm) {
      case CREATE_TABLE:
        tableName = tableNamePrefix + "__CREATE_TABLE_WITH_PERM_TEST__";
        loginAs(testUser);
        test_user_conn.tableOperations().create(tableName);
        loginAs(rootUser);
        if (!root_conn.tableOperations().list().contains(tableName))
          throw new IllegalStateException("Should be able to create a table");
        break;
      case DROP_TABLE:
        tableName = tableNamePrefix + "__DROP_TABLE_WITH_PERM_TEST__";
        loginAs(rootUser);
        root_conn.tableOperations().create(tableName);
        loginAs(testUser);
        test_user_conn.tableOperations().delete(tableName);
        loginAs(rootUser);
        if (root_conn.tableOperations().list().contains(tableName))
          throw new IllegalStateException("Should be able to delete a table");
        break;
      case ALTER_TABLE:
        tableName = tableNamePrefix + "__ALTER_TABLE_WITH_PERM_TEST__";
        String table2 = tableName + "2";
        loginAs(rootUser);
        root_conn.tableOperations().create(tableName);
        loginAs(testUser);
        test_user_conn.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
        loginAs(rootUser);
        Map<String,String> properties = map(root_conn.tableOperations().getProperties(tableName));
        if (!properties.get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
          throw new IllegalStateException("Should be able to set a table property");
        loginAs(testUser);
        test_user_conn.tableOperations().removeProperty(tableName, Property.TABLE_BLOOM_ERRORRATE.getKey());
        loginAs(rootUser);
        properties = map(root_conn.tableOperations().getProperties(tableName));
        if (properties.get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
          throw new IllegalStateException("Should be able to remove a table property");
        loginAs(testUser);
        test_user_conn.tableOperations().rename(tableName, table2);
        loginAs(rootUser);
        if (root_conn.tableOperations().list().contains(tableName) || !root_conn.tableOperations().list().contains(table2))
          throw new IllegalStateException("Should be able to rename a table");
        break;
      case CREATE_USER:
        user = "__CREATE_USER_WITH_PERM_TEST__";
        loginAs(testUser);
        test_user_conn.securityOperations().createLocalUser(user, (passwordBased ? new PasswordToken(password) : null));
        loginAs(rootUser);
        if (passwordBased && !root_conn.securityOperations().authenticateUser(user, new PasswordToken(password)))
          throw new IllegalStateException("Should be able to create a user");
        break;
      case DROP_USER:
        user = "__DROP_USER_WITH_PERM_TEST__";
        loginAs(rootUser);
        root_conn.securityOperations().createLocalUser(user, (passwordBased ? new PasswordToken(password) : null));
        loginAs(testUser);
        test_user_conn.securityOperations().dropLocalUser(user);
        loginAs(rootUser);
        if (passwordBased && root_conn.securityOperations().authenticateUser(user, new PasswordToken(password)))
          throw new IllegalStateException("Should be able to delete a user");
        break;
      case ALTER_USER:
        user = "__ALTER_USER_WITH_PERM_TEST__";
        loginAs(rootUser);
        root_conn.securityOperations().createLocalUser(user, (passwordBased ? new PasswordToken(password) : null));
        loginAs(testUser);
        test_user_conn.securityOperations().changeUserAuthorizations(user, new Authorizations("A", "B"));
        loginAs(rootUser);
        if (root_conn.securityOperations().getUserAuthorizations(user).isEmpty())
          throw new IllegalStateException("Should be able to alter a user");
        break;
      case SYSTEM:
        // test for system permission would go here
        break;
      case CREATE_NAMESPACE:
        namespace = "__CREATE_NAMESPACE_WITH_PERM_TEST__";
        loginAs(testUser);
        test_user_conn.namespaceOperations().create(namespace);
        loginAs(rootUser);
        if (!root_conn.namespaceOperations().list().contains(namespace))
          throw new IllegalStateException("Should be able to create a namespace");
        break;
      case DROP_NAMESPACE:
        namespace = "__DROP_NAMESPACE_WITH_PERM_TEST__";
        loginAs(rootUser);
        root_conn.namespaceOperations().create(namespace);
        loginAs(testUser);
        test_user_conn.namespaceOperations().delete(namespace);
        loginAs(rootUser);
        if (root_conn.namespaceOperations().list().contains(namespace))
          throw new IllegalStateException("Should be able to delete a namespace");
        break;
      case ALTER_NAMESPACE:
        namespace = "__ALTER_NAMESPACE_WITH_PERM_TEST__";
        String namespace2 = namespace + "2";
        loginAs(rootUser);
        root_conn.namespaceOperations().create(namespace);
        loginAs(testUser);
        test_user_conn.namespaceOperations().setProperty(namespace, Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
        loginAs(rootUser);
        Map<String,String> propies = map(root_conn.namespaceOperations().getProperties(namespace));
        if (!propies.get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
          throw new IllegalStateException("Should be able to set a table property");
        loginAs(testUser);
        test_user_conn.namespaceOperations().removeProperty(namespace, Property.TABLE_BLOOM_ERRORRATE.getKey());
        loginAs(rootUser);
        propies = map(root_conn.namespaceOperations().getProperties(namespace));
        if (propies.get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
          throw new IllegalStateException("Should be able to remove a table property");
        loginAs(testUser);
        test_user_conn.namespaceOperations().rename(namespace, namespace2);
        loginAs(rootUser);
        if (root_conn.namespaceOperations().list().contains(namespace) || !root_conn.namespaceOperations().list().contains(namespace2))
          throw new IllegalStateException("Should be able to rename a table");
        break;
      case OBTAIN_DELEGATION_TOKEN:
        ClientConfiguration clientConf = cluster.getClientConfig();
        if (clientConf.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
          // TODO Try to obtain a delegation token with the permission
        }
        break;
      case GRANT:
        loginAs(rootUser);
        root_conn.securityOperations().grantSystemPermission(testUser.getPrincipal(), SystemPermission.GRANT);
        loginAs(testUser);
        test_user_conn.securityOperations().grantSystemPermission(testUser.getPrincipal(), SystemPermission.CREATE_TABLE);
        loginAs(rootUser);
        assertTrue("Test user should have CREATE_TABLE",
            root_conn.securityOperations().hasSystemPermission(testUser.getPrincipal(), SystemPermission.CREATE_TABLE));
        assertTrue("Test user should have GRANT", root_conn.securityOperations().hasSystemPermission(testUser.getPrincipal(), SystemPermission.GRANT));
        root_conn.securityOperations().revokeSystemPermission(testUser.getPrincipal(), SystemPermission.CREATE_TABLE);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized System Permission: " + perm);
    }
  }

  private void verifyHasOnlyTheseSystemPermissions(Connector root_conn, String user, SystemPermission... perms) throws AccumuloException,
      AccumuloSecurityException {
    List<SystemPermission> permList = Arrays.asList(perms);
    for (SystemPermission p : SystemPermission.values()) {
      if (permList.contains(p)) {
        // should have these
        if (!root_conn.securityOperations().hasSystemPermission(user, p))
          throw new IllegalStateException(user + " SHOULD have system permission " + p);
      } else {
        // should not have these
        if (root_conn.securityOperations().hasSystemPermission(user, p))
          throw new IllegalStateException(user + " SHOULD NOT have system permission " + p);
      }
    }
  }

  private void verifyHasNoSystemPermissions(Connector root_conn, String user, SystemPermission... perms) throws AccumuloException, AccumuloSecurityException {
    for (SystemPermission p : perms)
      if (root_conn.securityOperations().hasSystemPermission(user, p))
        throw new IllegalStateException(user + " SHOULD NOT have system permission " + p);
  }

  @Test
  public void tablePermissionTest() throws Exception {
    // create the test user
    ClusterUser testUser = getUser(0), rootUser = getAdminUser();

    String principal = testUser.getPrincipal();
    AuthenticationToken token = testUser.getToken();
    PasswordToken passwordToken = null;
    if (token instanceof PasswordToken) {
      passwordToken = (PasswordToken) token;
    }
    loginAs(rootUser);
    Connector c = getConnector();
    c.securityOperations().createLocalUser(principal, passwordToken);
    loginAs(testUser);
    Connector test_user_conn = c.getInstance().getConnector(principal, token);

    // check for read-only access to metadata table
    loginAs(rootUser);
    verifyHasOnlyTheseTablePermissions(c, c.whoami(), MetadataTable.NAME, TablePermission.READ, TablePermission.ALTER_TABLE);
    verifyHasOnlyTheseTablePermissions(c, principal, MetadataTable.NAME, TablePermission.READ);
    String tableName = getUniqueNames(1)[0] + "__TABLE_PERMISSION_TEST__";

    // test each permission
    for (TablePermission perm : TablePermission.values()) {
      log.debug("Verifying the {} permission", perm);

      // test permission before and after granting it
      createTestTable(c, principal, tableName);
      loginAs(testUser);
      testMissingTablePermission(test_user_conn, testUser, perm, tableName);
      loginAs(rootUser);
      c.securityOperations().grantTablePermission(principal, tableName, perm);
      verifyHasOnlyTheseTablePermissions(c, principal, tableName, perm);
      loginAs(testUser);
      testGrantedTablePermission(test_user_conn, testUser, perm, tableName);

      loginAs(rootUser);
      createTestTable(c, principal, tableName);
      c.securityOperations().revokeTablePermission(principal, tableName, perm);
      verifyHasNoTablePermissions(c, principal, tableName, perm);
    }
  }

  private void createTestTable(Connector c, String testUser, String tableName) throws Exception, MutationsRejectedException {
    if (!c.tableOperations().exists(tableName)) {
      // create the test table
      c.tableOperations().create(tableName);
      // put in some initial data
      BatchWriter writer = c.createBatchWriter(tableName, new BatchWriterConfig());
      Mutation m = new Mutation(new Text("row"));
      m.put(new Text("cf"), new Text("cq"), new Value("val".getBytes()));
      writer.addMutation(m);
      writer.close();

      // verify proper permissions for creator and test user
      verifyHasOnlyTheseTablePermissions(c, c.whoami(), tableName, TablePermission.values());
      verifyHasNoTablePermissions(c, testUser, tableName, TablePermission.values());

    }
  }

  private void testMissingTablePermission(Connector test_user_conn, ClusterUser testUser, TablePermission perm, String tableName) throws Exception {
    BatchWriter writer;
    Mutation m;
    log.debug("Confirming that the lack of the {} permission properly restricts the user", perm);

    // test permission prior to granting it
    switch (perm) {
      case READ:
        try (Scanner scanner = test_user_conn.createScanner(tableName, Authorizations.EMPTY)) {
          int i = 0;
          for (Entry<Key,Value> entry : scanner)
            i += 1 + entry.getKey().getRowData().length();
          if (i != 0) {
            throw new IllegalStateException("Should NOT be able to read from the table");
          }
        } catch (RuntimeException e) {
          AccumuloSecurityException se = (AccumuloSecurityException) e.getCause();
          if (se.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw se;
        }
        break;
      case WRITE:
        try {
          writer = test_user_conn.createBatchWriter(tableName, new BatchWriterConfig());
          m = new Mutation(new Text("row"));
          m.put(new Text("a"), new Text("b"), new Value("c".getBytes()));
          writer.addMutation(m);
          try {
            writer.close();
          } catch (MutationsRejectedException e1) {
            if (e1.getSecurityErrorCodes().size() > 0)
              throw new AccumuloSecurityException(test_user_conn.whoami(), org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode.PERMISSION_DENIED, e1);
          }
          throw new IllegalStateException("Should NOT be able to write to a table");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        break;
      case BULK_IMPORT:
        // test for bulk import permission would go here
        break;
      case ALTER_TABLE:
        Map<String,Set<Text>> groups = new HashMap<>();
        groups.put("tgroup", new HashSet<>(Arrays.asList(new Text("t1"), new Text("t2"))));
        try {
          test_user_conn.tableOperations().setLocalityGroups(tableName, groups);
          throw new IllegalStateException("User should not be able to set locality groups");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        break;
      case DROP_TABLE:
        try {
          test_user_conn.tableOperations().delete(tableName);
          throw new IllegalStateException("User should not be able delete the table");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        break;
      case GRANT:
        try {
          test_user_conn.securityOperations().grantTablePermission(getAdminPrincipal(), tableName, TablePermission.GRANT);
          throw new IllegalStateException("User should not be able grant permissions");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        break;
      case GET_SUMMARIES:
        try {
          test_user_conn.tableOperations().summaries(tableName).retrieve();
          throw new IllegalStateException("User should not be able to get table summaries");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        break;
      default:
        throw new IllegalArgumentException("Unrecognized table Permission: " + perm);
    }
  }

  private void testGrantedTablePermission(Connector test_user_conn, ClusterUser normalUser, TablePermission perm, String tableName) throws AccumuloException,
      TableExistsException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException {
    BatchWriter writer;
    Mutation m;
    log.debug("Confirming that the presence of the {} permission properly permits the user", perm);

    // test permission after granting it
    switch (perm) {
      case READ:
        try (Scanner scanner = test_user_conn.createScanner(tableName, Authorizations.EMPTY)) {
          Iterator<Entry<Key,Value>> iter = scanner.iterator();
          while (iter.hasNext())
            iter.next();
        }
        break;
      case WRITE:
        writer = test_user_conn.createBatchWriter(tableName, new BatchWriterConfig());
        m = new Mutation(new Text("row"));
        m.put(new Text("a"), new Text("b"), new Value("c".getBytes()));
        writer.addMutation(m);
        writer.close();
        break;
      case BULK_IMPORT:
        // test for bulk import permission would go here
        break;
      case ALTER_TABLE:
        Map<String,Set<Text>> groups = new HashMap<>();
        groups.put("tgroup", new HashSet<>(Arrays.asList(new Text("t1"), new Text("t2"))));
        break;
      case DROP_TABLE:
        test_user_conn.tableOperations().delete(tableName);
        break;
      case GRANT:
        test_user_conn.securityOperations().grantTablePermission(getAdminPrincipal(), tableName, TablePermission.GRANT);
        break;
      case GET_SUMMARIES:
        List<Summary> summaries = test_user_conn.tableOperations().summaries(tableName).retrieve();
        // just make sure it's not blocked by permissions, the actual summaries are tested in SummaryIT
        Assert.assertTrue(summaries.isEmpty());
        break;
      default:
        throw new IllegalArgumentException("Unrecognized table Permission: " + perm);
    }
  }

  private void verifyHasOnlyTheseTablePermissions(Connector root_conn, String user, String table, TablePermission... perms) throws AccumuloException,
      AccumuloSecurityException {
    List<TablePermission> permList = Arrays.asList(perms);
    for (TablePermission p : TablePermission.values()) {
      if (permList.contains(p)) {
        // should have these
        if (!root_conn.securityOperations().hasTablePermission(user, table, p))
          throw new IllegalStateException(user + " SHOULD have table permission " + p + " for table " + table);
      } else {
        // should not have these
        if (root_conn.securityOperations().hasTablePermission(user, table, p))
          throw new IllegalStateException(user + " SHOULD NOT have table permission " + p + " for table " + table);
      }
    }
  }

  private void verifyHasNoTablePermissions(Connector root_conn, String user, String table, TablePermission... perms) throws AccumuloException,
      AccumuloSecurityException {
    for (TablePermission p : perms)
      if (root_conn.securityOperations().hasTablePermission(user, table, p))
        throw new IllegalStateException(user + " SHOULD NOT have table permission " + p + " for table " + table);
  }
}
