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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
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
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test verifies the default permissions so a clean instance must be used. A shared instance
 * might not be representative of a fresh installation.
 */
@Category(MiniClusterOnlyTests.class)
public class PermissionsIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(PermissionsIT.class);

  @Override
  public int defaultTimeoutSeconds() {
    return 90;
  }

  @Before
  public void limitToMini() throws Exception {
    Assume.assumeTrue(getClusterType() == ClusterType.MINI);
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      Set<String> users = c.securityOperations().listLocalUsers();
      ClusterUser user = getUser(0);
      if (users.contains(user.getPrincipal())) {
        c.securityOperations().dropLocalUser(user.getPrincipal());
      }
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
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
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
      try (AccumuloClient test_user_client =
          Accumulo.newClient().from(c.properties()).as(principal, token).build()) {
        loginAs(rootUser);
        verifyHasNoSystemPermissions(c, principal, SystemPermission.values());

        // test each permission
        for (SystemPermission perm : SystemPermission.values()) {
          log.debug("Verifying the {} permission", perm);

          // test permission before and after granting it
          String tableNamePrefix = getUniqueNames(1)[0];
          testMissingSystemPermission(tableNamePrefix, c, rootUser, test_user_client, testUser,
              perm);
          loginAs(rootUser);
          c.securityOperations().grantSystemPermission(principal, perm);
          verifyHasOnlyTheseSystemPermissions(c, principal, perm);
          testGrantedSystemPermission(tableNamePrefix, c, rootUser, test_user_client, testUser,
              perm);
          loginAs(rootUser);
          c.securityOperations().revokeSystemPermission(principal, perm);
          verifyHasNoSystemPermissions(c, principal, perm);
        }
      }
    }
  }

  static Map<String,String> map(Iterable<Entry<String,String>> i) {
    Map<String,String> result = new HashMap<>();
    for (Entry<String,String> e : i) {
      result.put(e.getKey(), e.getValue());
    }
    return result;
  }

  private void testMissingSystemPermission(String tableNamePrefix, AccumuloClient root_client,
      ClusterUser rootUser, AccumuloClient test_user_client, ClusterUser testUser,
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
          test_user_client.tableOperations().create(tableName);
          throw new IllegalStateException("Should NOT be able to create a table");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || root_client.tableOperations().list().contains(tableName))
            throw e;
        }
        break;
      case DROP_TABLE:
        tableName = tableNamePrefix + "__DROP_TABLE_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_client.tableOperations().create(tableName);
        try {
          loginAs(testUser);
          test_user_client.tableOperations().delete(tableName);
          throw new IllegalStateException("Should NOT be able to delete a table");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !root_client.tableOperations().list().contains(tableName))
            throw e;
        }
        break;
      case ALTER_TABLE:
        tableName = tableNamePrefix + "__ALTER_TABLE_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_client.tableOperations().create(tableName);
        try {
          loginAs(testUser);
          test_user_client.tableOperations().setProperty(tableName,
              Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
          throw new IllegalStateException("Should NOT be able to set a table property");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || map(root_client.tableOperations().getProperties(tableName))
                  .get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
            throw e;
        }
        loginAs(rootUser);
        root_client.tableOperations().setProperty(tableName,
            Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
        try {
          loginAs(testUser);
          test_user_client.tableOperations().removeProperty(tableName,
              Property.TABLE_BLOOM_ERRORRATE.getKey());
          throw new IllegalStateException("Should NOT be able to remove a table property");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !map(root_client.tableOperations().getProperties(tableName))
                  .get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
            throw e;
        }
        String table2 = tableName + "2";
        try {
          loginAs(testUser);
          test_user_client.tableOperations().rename(tableName, table2);
          throw new IllegalStateException("Should NOT be able to rename a table");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !root_client.tableOperations().list().contains(tableName)
              || root_client.tableOperations().list().contains(table2))
            throw e;
        }
        break;
      case CREATE_USER:
        user = "__CREATE_USER_WITHOUT_PERM_TEST__";
        try {
          loginAs(testUser);
          test_user_client.securityOperations().createLocalUser(user,
              (passwordBased ? new PasswordToken(password) : null));
          throw new IllegalStateException("Should NOT be able to create a user");
        } catch (AccumuloSecurityException e) {
          AuthenticationToken userToken = testUser.getToken();
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || (userToken instanceof PasswordToken
                  && root_client.securityOperations().authenticateUser(user, userToken)))
            throw e;
        }
        break;
      case DROP_USER:
        user = "__DROP_USER_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_client.securityOperations().createLocalUser(user,
            (passwordBased ? new PasswordToken(password) : null));
        try {
          loginAs(testUser);
          test_user_client.securityOperations().dropLocalUser(user);
          throw new IllegalStateException("Should NOT be able to delete a user");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !root_client.securityOperations().listLocalUsers().contains(user)) {
            log.info("Failed to authenticate as {}", user);
            throw e;
          }
        }
        break;
      case ALTER_USER:
        user = "__ALTER_USER_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_client.securityOperations().createLocalUser(user,
            (passwordBased ? new PasswordToken(password) : null));
        try {
          loginAs(testUser);
          test_user_client.securityOperations().changeUserAuthorizations(user,
              new Authorizations("A", "B"));
          throw new IllegalStateException("Should NOT be able to alter a user");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !root_client.securityOperations().getUserAuthorizations(user).isEmpty())
            throw e;
        }
        break;
      case SYSTEM:
        try {
          // Test setProperty
          loginAs(testUser);
          test_user_client.instanceOperations()
              .setProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey(), "10000");
          throw new IllegalStateException("Should NOT be able to set System Property");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || root_client.instanceOperations().getSystemConfiguration()
                  .get(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey()).equals("10000"))
            throw e;
        }
        // Test removal of property
        loginAs(rootUser);
        root_client.instanceOperations()
            .setProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey(), "10000");
        try {
          loginAs(testUser);
          test_user_client.instanceOperations()
              .removeProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey());
          throw new IllegalStateException("Should NOT be able to remove Sysem Property");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !root_client.instanceOperations().getSystemConfiguration()
                  .get(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey()).equals("10000"))
            throw e;
        }
        break;
      case CREATE_NAMESPACE:
        namespace = "__CREATE_NAMESPACE_WITHOUT_PERM_TEST__";
        try {
          loginAs(testUser);
          test_user_client.namespaceOperations().create(namespace);
          throw new IllegalStateException("Should NOT be able to create a namespace");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || root_client.namespaceOperations().list().contains(namespace))
            throw e;
        }
        break;
      case DROP_NAMESPACE:
        namespace = "__DROP_NAMESPACE_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_client.namespaceOperations().create(namespace);
        try {
          loginAs(testUser);
          test_user_client.namespaceOperations().delete(namespace);
          throw new IllegalStateException("Should NOT be able to delete a namespace");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !root_client.namespaceOperations().list().contains(namespace))
            throw e;
        }
        break;
      case ALTER_NAMESPACE:
        namespace = "__ALTER_NAMESPACE_WITHOUT_PERM_TEST__";
        loginAs(rootUser);
        root_client.namespaceOperations().create(namespace);
        try {
          loginAs(testUser);
          test_user_client.namespaceOperations().setProperty(namespace,
              Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
          throw new IllegalStateException("Should NOT be able to set a namespace property");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || map(root_client.namespaceOperations().getProperties(namespace))
                  .get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
            throw e;
        }
        loginAs(rootUser);
        root_client.namespaceOperations().setProperty(namespace,
            Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
        try {
          loginAs(testUser);
          test_user_client.namespaceOperations().removeProperty(namespace,
              Property.TABLE_BLOOM_ERRORRATE.getKey());
          throw new IllegalStateException("Should NOT be able to remove a namespace property");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !map(root_client.namespaceOperations().getProperties(namespace))
                  .get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
            throw e;
        }
        String namespace2 = namespace + "2";
        try {
          loginAs(testUser);
          test_user_client.namespaceOperations().rename(namespace, namespace2);
          throw new IllegalStateException("Should NOT be able to rename a namespace");
        } catch (AccumuloSecurityException e) {
          loginAs(rootUser);
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !root_client.namespaceOperations().list().contains(namespace)
              || root_client.namespaceOperations().list().contains(namespace2))
            throw e;
        }
        break;
      case OBTAIN_DELEGATION_TOKEN:
        if (saslEnabled()) {
          // TODO Try to obtain a delegation token without the permission
        }
        break;
      case GRANT:
        loginAs(testUser);
        try {
          test_user_client.securityOperations().grantSystemPermission(testUser.getPrincipal(),
              SystemPermission.GRANT);
          throw new IllegalStateException("Should NOT be able to grant System.GRANT to yourself");
        } catch (AccumuloSecurityException e) {
          // Expected
          loginAs(rootUser);
          assertFalse(root_client.securityOperations().hasSystemPermission(testUser.getPrincipal(),
              SystemPermission.GRANT));
        }
        break;
      default:
        throw new IllegalArgumentException("Unrecognized System Permission: " + perm);
    }
  }

  private void testGrantedSystemPermission(String tableNamePrefix, AccumuloClient root_client,
      ClusterUser rootUser, AccumuloClient test_user_client, ClusterUser testUser,
      SystemPermission perm) throws Exception {
    String tableName, user, password = "password", namespace;
    boolean passwordBased = testUser.getPassword() != null;
    log.debug("Confirming that the presence of the {} permission properly permits the user", perm);

    // test permission after granting it
    switch (perm) {
      case CREATE_TABLE:
        tableName = tableNamePrefix + "__CREATE_TABLE_WITH_PERM_TEST__";
        loginAs(testUser);
        test_user_client.tableOperations().create(tableName);
        loginAs(rootUser);
        if (!root_client.tableOperations().list().contains(tableName))
          throw new IllegalStateException("Should be able to create a table");
        break;
      case DROP_TABLE:
        tableName = tableNamePrefix + "__DROP_TABLE_WITH_PERM_TEST__";
        loginAs(rootUser);
        root_client.tableOperations().create(tableName);
        loginAs(testUser);
        test_user_client.tableOperations().delete(tableName);
        loginAs(rootUser);
        if (root_client.tableOperations().list().contains(tableName))
          throw new IllegalStateException("Should be able to delete a table");
        break;
      case ALTER_TABLE:
        tableName = tableNamePrefix + "__ALTER_TABLE_WITH_PERM_TEST__";
        String table2 = tableName + "2";
        loginAs(rootUser);
        root_client.tableOperations().create(tableName);
        testArbitraryProperty(root_client, tableName, true);
        loginAs(testUser);
        test_user_client.tableOperations().setProperty(tableName,
            Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
        loginAs(rootUser);
        Map<String,String> properties = map(root_client.tableOperations().getProperties(tableName));
        if (!properties.get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
          throw new IllegalStateException("Should be able to set a table property");
        loginAs(testUser);
        test_user_client.tableOperations().removeProperty(tableName,
            Property.TABLE_BLOOM_ERRORRATE.getKey());
        loginAs(rootUser);
        properties = map(root_client.tableOperations().getProperties(tableName));
        if (properties.get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
          throw new IllegalStateException("Should be able to remove a table property");
        loginAs(testUser);
        test_user_client.tableOperations().rename(tableName, table2);
        loginAs(rootUser);
        if (root_client.tableOperations().list().contains(tableName)
            || !root_client.tableOperations().list().contains(table2))
          throw new IllegalStateException("Should be able to rename a table");
        break;
      case CREATE_USER:
        user = "__CREATE_USER_WITH_PERM_TEST__";
        loginAs(testUser);
        test_user_client.securityOperations().createLocalUser(user,
            (passwordBased ? new PasswordToken(password) : null));
        loginAs(rootUser);
        if (passwordBased && !root_client.securityOperations().authenticateUser(user,
            new PasswordToken(password)))
          throw new IllegalStateException("Should be able to create a user");
        break;
      case DROP_USER:
        user = "__DROP_USER_WITH_PERM_TEST__";
        loginAs(rootUser);
        root_client.securityOperations().createLocalUser(user,
            (passwordBased ? new PasswordToken(password) : null));
        loginAs(testUser);
        test_user_client.securityOperations().dropLocalUser(user);
        loginAs(rootUser);
        if (passwordBased
            && root_client.securityOperations().authenticateUser(user, new PasswordToken(password)))
          throw new IllegalStateException("Should be able to delete a user");
        break;
      case ALTER_USER:
        user = "__ALTER_USER_WITH_PERM_TEST__";
        loginAs(rootUser);
        root_client.securityOperations().createLocalUser(user,
            (passwordBased ? new PasswordToken(password) : null));
        loginAs(testUser);
        test_user_client.securityOperations().changeUserAuthorizations(user,
            new Authorizations("A", "B"));
        loginAs(rootUser);
        if (root_client.securityOperations().getUserAuthorizations(user).isEmpty())
          throw new IllegalStateException("Should be able to alter a user");
        break;
      case SYSTEM:
        // Test setProperty
        loginAs(testUser);
        test_user_client.instanceOperations()
            .setProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey(), "10000");
        loginAs(rootUser);
        if (!root_client.instanceOperations().getSystemConfiguration()
            .get(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey()).equals("10000"))
          throw new IllegalStateException("Should be able to set system property");
        // Test removal of property
        loginAs(testUser);
        test_user_client.instanceOperations()
            .removeProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey());
        loginAs(rootUser);
        if (root_client.instanceOperations().getSystemConfiguration()
            .get(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey()).equals("10000"))
          throw new IllegalStateException("Should be able remove systemproperty");
        break;
      case CREATE_NAMESPACE:
        namespace = "__CREATE_NAMESPACE_WITH_PERM_TEST__";
        loginAs(testUser);
        test_user_client.namespaceOperations().create(namespace);
        loginAs(rootUser);
        if (!root_client.namespaceOperations().list().contains(namespace))
          throw new IllegalStateException("Should be able to create a namespace");
        break;
      case DROP_NAMESPACE:
        namespace = "__DROP_NAMESPACE_WITH_PERM_TEST__";
        loginAs(rootUser);
        root_client.namespaceOperations().create(namespace);
        loginAs(testUser);
        test_user_client.namespaceOperations().delete(namespace);
        loginAs(rootUser);
        if (root_client.namespaceOperations().list().contains(namespace))
          throw new IllegalStateException("Should be able to delete a namespace");
        break;
      case ALTER_NAMESPACE:
        namespace = "__ALTER_NAMESPACE_WITH_PERM_TEST__";
        String namespace2 = namespace + "2";
        loginAs(rootUser);
        root_client.namespaceOperations().create(namespace);
        loginAs(testUser);
        test_user_client.namespaceOperations().setProperty(namespace,
            Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
        loginAs(rootUser);
        Map<String,String> propies =
            map(root_client.namespaceOperations().getProperties(namespace));
        if (!propies.get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
          throw new IllegalStateException("Should be able to set a table property");
        loginAs(testUser);
        test_user_client.namespaceOperations().removeProperty(namespace,
            Property.TABLE_BLOOM_ERRORRATE.getKey());
        loginAs(rootUser);
        propies = map(root_client.namespaceOperations().getProperties(namespace));
        if (propies.get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
          throw new IllegalStateException("Should be able to remove a table property");
        loginAs(testUser);
        test_user_client.namespaceOperations().rename(namespace, namespace2);
        loginAs(rootUser);
        if (root_client.namespaceOperations().list().contains(namespace)
            || !root_client.namespaceOperations().list().contains(namespace2))
          throw new IllegalStateException("Should be able to rename a table");
        break;
      case OBTAIN_DELEGATION_TOKEN:
        if (saslEnabled()) {
          // TODO Try to obtain a delegation token with the permission
        }
        break;
      case GRANT:
        loginAs(rootUser);
        root_client.securityOperations().grantSystemPermission(testUser.getPrincipal(),
            SystemPermission.GRANT);
        loginAs(testUser);
        test_user_client.securityOperations().grantSystemPermission(testUser.getPrincipal(),
            SystemPermission.CREATE_TABLE);
        loginAs(rootUser);
        assertTrue("Test user should have CREATE_TABLE", root_client.securityOperations()
            .hasSystemPermission(testUser.getPrincipal(), SystemPermission.CREATE_TABLE));
        assertTrue("Test user should have GRANT", root_client.securityOperations()
            .hasSystemPermission(testUser.getPrincipal(), SystemPermission.GRANT));
        root_client.securityOperations().revokeSystemPermission(testUser.getPrincipal(),
            SystemPermission.CREATE_TABLE);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized System Permission: " + perm);
    }
  }

  private void verifyHasOnlyTheseSystemPermissions(AccumuloClient root_client, String user,
      SystemPermission... perms) throws AccumuloException, AccumuloSecurityException {
    List<SystemPermission> permList = Arrays.asList(perms);
    for (SystemPermission p : SystemPermission.values()) {
      if (permList.contains(p)) {
        // should have these
        if (!root_client.securityOperations().hasSystemPermission(user, p))
          throw new IllegalStateException(user + " SHOULD have system permission " + p);
      } else {
        // should not have these
        if (root_client.securityOperations().hasSystemPermission(user, p))
          throw new IllegalStateException(user + " SHOULD NOT have system permission " + p);
      }
    }
  }

  private void verifyHasNoSystemPermissions(AccumuloClient root_client, String user,
      SystemPermission... perms) throws AccumuloException, AccumuloSecurityException {
    for (SystemPermission p : perms)
      if (root_client.securityOperations().hasSystemPermission(user, p))
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
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.securityOperations().createLocalUser(principal, passwordToken);
      loginAs(testUser);
      try (AccumuloClient test_user_client =
          Accumulo.newClient().from(c.properties()).as(principal, token).build()) {

        // check for read-only access to metadata table
        loginAs(rootUser);
        verifyHasOnlyTheseTablePermissions(c, c.whoami(), MetadataTable.NAME, TablePermission.READ,
            TablePermission.ALTER_TABLE);
        verifyHasOnlyTheseTablePermissions(c, principal, MetadataTable.NAME, TablePermission.READ);
        String tableName = getUniqueNames(1)[0] + "__TABLE_PERMISSION_TEST__";

        // test each permission
        for (TablePermission perm : TablePermission.values()) {
          log.debug("Verifying the {} permission", perm);

          // test permission before and after granting it
          createTestTable(c, principal, tableName);
          loginAs(testUser);
          testMissingTablePermission(test_user_client, perm, tableName);
          loginAs(rootUser);
          c.securityOperations().grantTablePermission(principal, tableName, perm);
          verifyHasOnlyTheseTablePermissions(c, principal, tableName, perm);
          loginAs(testUser);
          testGrantedTablePermission(test_user_client, perm, tableName);

          loginAs(rootUser);
          createTestTable(c, principal, tableName);
          c.securityOperations().revokeTablePermission(principal, tableName, perm);
          verifyHasNoTablePermissions(c, principal, tableName, perm);
        }
      }
    }
  }

  private void createTestTable(AccumuloClient c, String testUser, String tableName)
      throws Exception {
    if (!c.tableOperations().exists(tableName)) {
      // create the test table
      c.tableOperations().create(tableName);
      // put in some initial data
      try (BatchWriter writer = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation(new Text("row"));
        m.put("cf", "cq", "val");
        writer.addMutation(m);
      }

      // verify proper permissions for creator and test user
      verifyHasOnlyTheseTablePermissions(c, c.whoami(), tableName, TablePermission.values());
      verifyHasNoTablePermissions(c, testUser, tableName, TablePermission.values());

    }
  }

  private void testMissingTablePermission(AccumuloClient test_user_client, TablePermission perm,
      String tableName) throws Exception {
    Mutation m;
    log.debug("Confirming that the lack of the {} permission properly restricts the user", perm);

    // test permission prior to granting it
    switch (perm) {
      case READ:
        try (Scanner scanner = test_user_client.createScanner(tableName, Authorizations.EMPTY)) {
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
          try (BatchWriter bw = test_user_client.createBatchWriter(tableName)) {
            m = new Mutation(new Text("row"));
            m.put("a", "b", "c");
            bw.addMutation(m);
          } catch (MutationsRejectedException e1) {
            if (!e1.getSecurityErrorCodes().isEmpty())
              throw new AccumuloSecurityException(test_user_client.whoami(),
                  org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode.PERMISSION_DENIED,
                  e1);
          }
          throw new IllegalStateException("Should NOT be able to write to a table");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        // Now see if we can flush
        try {
          test_user_client.tableOperations().flush(tableName, new Text("myrow"), new Text("myrow~"),
              false);
          throw new IllegalStateException("Should NOT be able to flsuh a table");
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
          test_user_client.tableOperations().setLocalityGroups(tableName, groups);
          throw new IllegalStateException("User should not be able to set locality groups");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        try {
          test_user_client.tableOperations().flush(tableName, new Text("myrow"), new Text("myrow~"),
              false);
          throw new IllegalStateException("Should NOT be able to flsuh a table");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        testArbitraryProperty(test_user_client, tableName, false);
        break;
      case DROP_TABLE:
        try {
          test_user_client.tableOperations().delete(tableName);
          throw new IllegalStateException("User should not be able delete the table");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        break;
      case GRANT:
        try {
          test_user_client.securityOperations().grantTablePermission(getAdminPrincipal(), tableName,
              TablePermission.GRANT);
          throw new IllegalStateException("User should not be able grant permissions");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        break;
      case GET_SUMMARIES:
        try {
          test_user_client.tableOperations().summaries(tableName).retrieve();
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

  private void testGrantedTablePermission(AccumuloClient test_user_client, TablePermission perm,
      String tableName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    log.debug("Confirming that the presence of the {} permission properly permits the user", perm);

    // test permission after granting it
    switch (perm) {
      case READ:
        try (Scanner scanner = test_user_client.createScanner(tableName, Authorizations.EMPTY)) {
          for (Entry<Key,Value> keyValueEntry : scanner) {
            assertNotNull(keyValueEntry);
          }
        }
        break;
      case WRITE:
        test_user_client.tableOperations().flush(tableName, new Text("myrow"), new Text("myrow~"),
            false);
        try (BatchWriter bw = test_user_client.createBatchWriter(tableName)) {
          Mutation m = new Mutation(new Text("row"));
          m.put("a", "b", "c");
          bw.addMutation(m);
        }
        break;
      case BULK_IMPORT:
        // test for bulk import permission would go here
        break;
      case ALTER_TABLE:
        test_user_client.tableOperations().flush(tableName, new Text("myrow"), new Text("myrow~"),
            false);
        testArbitraryProperty(test_user_client, tableName, true);
        break;
      case DROP_TABLE:
        test_user_client.tableOperations().delete(tableName);
        break;
      case GRANT:
        test_user_client.securityOperations().grantTablePermission(getAdminPrincipal(), tableName,
            TablePermission.GRANT);
        break;
      case GET_SUMMARIES:
        List<Summary> summaries =
            test_user_client.tableOperations().summaries(tableName).retrieve();
        // just make sure it's not blocked by permissions, the actual summaries are tested in
        // SummaryIT
        assertTrue(summaries.isEmpty());
        break;
      default:
        throw new IllegalArgumentException("Unrecognized table Permission: " + perm);
    }
  }

  private void verifyHasOnlyTheseTablePermissions(AccumuloClient root_client, String user,
      String table, TablePermission... perms) throws AccumuloException, AccumuloSecurityException {
    List<TablePermission> permList = Arrays.asList(perms);
    for (TablePermission p : TablePermission.values()) {
      if (permList.contains(p)) {
        // should have these
        if (!root_client.securityOperations().hasTablePermission(user, table, p))
          throw new IllegalStateException(
              user + " SHOULD have table permission " + p + " for table " + table);
      } else {
        // should not have these
        if (root_client.securityOperations().hasTablePermission(user, table, p))
          throw new IllegalStateException(
              user + " SHOULD NOT have table permission " + p + " for table " + table);
      }
    }
  }

  private void verifyHasNoTablePermissions(AccumuloClient root_client, String user, String table,
      TablePermission... perms) throws AccumuloException, AccumuloSecurityException {
    for (TablePermission p : perms)
      if (root_client.securityOperations().hasTablePermission(user, table, p))
        throw new IllegalStateException(
            user + " SHOULD NOT have table permission " + p + " for table " + table);
  }

  private void testArbitraryProperty(AccumuloClient c, String tableName, boolean havePerm)
      throws AccumuloException, TableNotFoundException {
    // Set variables for the property name to use and the initial value
    String propertyName = "table.custom.description";
    String description1 = "Description";

    // Make sure the property name is valid
    assertTrue(Property.isValidPropertyKey(propertyName));
    // Set the property to the desired value
    try {
      c.tableOperations().setProperty(tableName, propertyName, description1);

      // Loop through properties to make sure the new property is added to the list
      int count = 0;
      for (Entry<String,String> property : c.tableOperations().getProperties(tableName)) {
        if (property.getKey().equals(propertyName) && property.getValue().equals(description1))
          count++;
      }
      assertEquals(count, 1);

      // Set the property as something different
      String description2 = "set second";
      c.tableOperations().setProperty(tableName, propertyName, description2);

      // Loop through properties to make sure the new property is added to the list
      count = 0;
      for (Entry<String,String> property : c.tableOperations().getProperties(tableName)) {
        if (property.getKey().equals(propertyName) && property.getValue().equals(description2))
          count++;
      }
      assertEquals(count, 1);

      // Remove the property and make sure there is no longer a value associated with it
      c.tableOperations().removeProperty(tableName, propertyName);

      // Loop through properties to make sure the new property is added to the list
      count = 0;
      for (Entry<String,String> property : c.tableOperations().getProperties(tableName)) {
        if (property.getKey().equals(propertyName))
          count++;
      }
      assertEquals(count, 0);
      if (!havePerm)
        throw new IllegalStateException("User should not been able to alter property.");
    } catch (AccumuloSecurityException se) {
      if (havePerm)
        throw new IllegalStateException("User should have been able to alter property");
    }
  }
}
