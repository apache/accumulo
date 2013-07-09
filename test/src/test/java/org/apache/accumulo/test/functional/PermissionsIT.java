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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class PermissionsIT extends MacTest {
  private static final String TEST_USER = "test_user";
  private static final PasswordToken TEST_PASS = new PasswordToken("test_password");
  
  @Test(timeout = 60 * 1000)
  public void systemPermissionsTest() throws Exception {
    // verify that the test is being run by root
    Connector c = getConnector();
    verifyHasOnlyTheseSystemPermissions(c, c.whoami(), SystemPermission.values());
    
    // create the test user
    c.securityOperations().createLocalUser(TEST_USER, TEST_PASS);
    Connector test_user_conn = c.getInstance().getConnector(TEST_USER, TEST_PASS);
    verifyHasNoSystemPermissions(c, TEST_USER, SystemPermission.values());
    
    // test each permission
    for (SystemPermission perm : SystemPermission.values()) {
      log.debug("Verifying the " + perm + " permission");
      
      // verify GRANT can't be granted
      if (perm.equals(SystemPermission.GRANT)) {
        try {
          c.securityOperations().grantSystemPermission(TEST_USER, perm);
        } catch (AccumuloSecurityException e) {
          verifyHasNoSystemPermissions(c, TEST_USER, perm);
          continue;
        }
        throw new IllegalStateException("Should NOT be able to grant GRANT");
      }
      
      // test permission before and after granting it
      testMissingSystemPermission(c, test_user_conn, perm);
      c.securityOperations().grantSystemPermission(TEST_USER, perm);
      verifyHasOnlyTheseSystemPermissions(c, TEST_USER, perm);
      testGrantedSystemPermission(c, test_user_conn, perm);
      c.securityOperations().revokeSystemPermission(TEST_USER, perm);
      verifyHasNoSystemPermissions(c, TEST_USER, perm);
    }
  }
  
  static Map<String,String> map(Iterable<Entry<String,String>> i) {
    Map<String,String> result = new HashMap<String,String>();
    for (Entry<String,String> e : i) {
      result.put(e.getKey(), e.getValue());
    }
    return result;
  }
  
  private static void testMissingSystemPermission(Connector root_conn, Connector test_user_conn, SystemPermission perm) throws AccumuloException,
      TableExistsException, AccumuloSecurityException, TableNotFoundException {
    String tableName, user, password = "password";
    log.debug("Confirming that the lack of the " + perm + " permission properly restricts the user");
    
    // test permission prior to granting it
    switch (perm) {
      case CREATE_TABLE:
        tableName = "__CREATE_TABLE_WITHOUT_PERM_TEST__";
        try {
          test_user_conn.tableOperations().create(tableName);
          throw new IllegalStateException("Should NOT be able to create a table");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || root_conn.tableOperations().list().contains(tableName))
            throw e;
        }
        break;
      case DROP_TABLE:
        tableName = "__DROP_TABLE_WITHOUT_PERM_TEST__";
        root_conn.tableOperations().create(tableName);
        try {
          test_user_conn.tableOperations().delete(tableName);
          throw new IllegalStateException("Should NOT be able to delete a table");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || !root_conn.tableOperations().list().contains(tableName))
            throw e;
        }
        break;
      case ALTER_TABLE:
        tableName = "__ALTER_TABLE_WITHOUT_PERM_TEST__";
        root_conn.tableOperations().create(tableName);
        try {
          test_user_conn.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
          throw new IllegalStateException("Should NOT be able to set a table property");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || map(root_conn.tableOperations().getProperties(tableName)).get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
            throw e;
        }
        root_conn.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
        try {
          test_user_conn.tableOperations().removeProperty(tableName, Property.TABLE_BLOOM_ERRORRATE.getKey());
          throw new IllegalStateException("Should NOT be able to remove a table property");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !map(root_conn.tableOperations().getProperties(tableName)).get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
            throw e;
        }
        String table2 = tableName + "2";
        try {
          test_user_conn.tableOperations().rename(tableName, table2);
          throw new IllegalStateException("Should NOT be able to rename a table");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || !root_conn.tableOperations().list().contains(tableName)
              || root_conn.tableOperations().list().contains(table2))
            throw e;
        }
        break;
      case CREATE_USER:
        user = "__CREATE_USER_WITHOUT_PERM_TEST__";
        try {
          test_user_conn.securityOperations().createLocalUser(user, new PasswordToken(password));
          throw new IllegalStateException("Should NOT be able to create a user");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || root_conn.securityOperations().authenticateUser(user, new PasswordToken(password)))
            throw e;
        }
        break;
      case DROP_USER:
        user = "__DROP_USER_WITHOUT_PERM_TEST__";
        root_conn.securityOperations().createLocalUser(user, new PasswordToken(password));
        try {
          test_user_conn.securityOperations().dropLocalUser(user);
          throw new IllegalStateException("Should NOT be able to delete a user");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED
              || !root_conn.securityOperations().authenticateUser(user, new PasswordToken(password)))
            throw e;
        }
        break;
      case ALTER_USER:
        user = "__ALTER_USER_WITHOUT_PERM_TEST__";
        root_conn.securityOperations().createLocalUser(user, new PasswordToken(password));
        try {
          test_user_conn.securityOperations().changeUserAuthorizations(user, new Authorizations("A", "B"));
          throw new IllegalStateException("Should NOT be able to alter a user");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED || !root_conn.securityOperations().getUserAuthorizations(user).isEmpty())
            throw e;
        }
        break;
      case SYSTEM:
        // test for system permission would go here
        break;
      default:
        throw new IllegalArgumentException("Unrecognized System Permission: " + perm);
    }
  }
  
  private static void testGrantedSystemPermission(Connector root_conn, Connector test_user_conn, SystemPermission perm) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, TableExistsException {
    String tableName, user, password = "password";
    log.debug("Confirming that the presence of the " + perm + " permission properly permits the user");
    
    // test permission after granting it
    switch (perm) {
      case CREATE_TABLE:
        tableName = "__CREATE_TABLE_WITH_PERM_TEST__";
        test_user_conn.tableOperations().create(tableName);
        if (!root_conn.tableOperations().list().contains(tableName))
          throw new IllegalStateException("Should be able to create a table");
        break;
      case DROP_TABLE:
        tableName = "__DROP_TABLE_WITH_PERM_TEST__";
        root_conn.tableOperations().create(tableName);
        test_user_conn.tableOperations().delete(tableName);
        if (root_conn.tableOperations().list().contains(tableName))
          throw new IllegalStateException("Should be able to delete a table");
        break;
      case ALTER_TABLE:
        tableName = "__ALTER_TABLE_WITH_PERM_TEST__";
        String table2 = tableName + "2";
        root_conn.tableOperations().create(tableName);
        test_user_conn.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ERRORRATE.getKey(), "003.14159%");
        Map<String,String> properties = map(root_conn.tableOperations().getProperties(tableName));
        if (!properties.get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
          throw new IllegalStateException("Should be able to set a table property");
        test_user_conn.tableOperations().removeProperty(tableName, Property.TABLE_BLOOM_ERRORRATE.getKey());
        properties = map(root_conn.tableOperations().getProperties(tableName));
        if (properties.get(Property.TABLE_BLOOM_ERRORRATE.getKey()).equals("003.14159%"))
          throw new IllegalStateException("Should be able to remove a table property");
        test_user_conn.tableOperations().rename(tableName, table2);
        if (root_conn.tableOperations().list().contains(tableName) || !root_conn.tableOperations().list().contains(table2))
          throw new IllegalStateException("Should be able to rename a table");
        break;
      case CREATE_USER:
        user = "__CREATE_USER_WITH_PERM_TEST__";
        test_user_conn.securityOperations().createLocalUser(user, new PasswordToken(password));
        if (!root_conn.securityOperations().authenticateUser(user, new PasswordToken(password)))
          throw new IllegalStateException("Should be able to create a user");
        break;
      case DROP_USER:
        user = "__DROP_USER_WITH_PERM_TEST__";
        root_conn.securityOperations().createLocalUser(user, new PasswordToken(password));
        test_user_conn.securityOperations().dropLocalUser(user);
        if (root_conn.securityOperations().authenticateUser(user, new PasswordToken(password)))
          throw new IllegalStateException("Should be able to delete a user");
        break;
      case ALTER_USER:
        user = "__ALTER_USER_WITH_PERM_TEST__";
        root_conn.securityOperations().createLocalUser(user, new PasswordToken(password));
        test_user_conn.securityOperations().changeUserAuthorizations(user, new Authorizations("A", "B"));
        if (root_conn.securityOperations().getUserAuthorizations(user).isEmpty())
          throw new IllegalStateException("Should be able to alter a user");
        break;
      case SYSTEM:
        // test for system permission would go here
        break;
      default:
        throw new IllegalArgumentException("Unrecognized System Permission: " + perm);
    }
  }
  
  private static void verifyHasOnlyTheseSystemPermissions(Connector root_conn, String user, SystemPermission... perms) throws AccumuloException,
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
  
  private static void verifyHasNoSystemPermissions(Connector root_conn, String user, SystemPermission... perms) throws AccumuloException,
      AccumuloSecurityException {
    for (SystemPermission p : perms)
      if (root_conn.securityOperations().hasSystemPermission(user, p))
        throw new IllegalStateException(user + " SHOULD NOT have system permission " + p);
  }
  
  private static final String TEST_TABLE = "__TABLE_PERMISSION_TEST__";
  
  @Test(timeout=30*1000)
  public void tablePermissionTest() throws Exception {
    // create the test user
    Connector c = getConnector();
    c.securityOperations().createLocalUser(TEST_USER, TEST_PASS);
    Connector test_user_conn = c.getInstance().getConnector(TEST_USER, TEST_PASS);
    
    // check for read-only access to metadata table
    verifyHasOnlyTheseTablePermissions(c, c.whoami(), MetadataTable.NAME, TablePermission.READ, TablePermission.ALTER_TABLE);
    verifyHasOnlyTheseTablePermissions(c, TEST_USER, MetadataTable.NAME, TablePermission.READ);
    
    // test each permission
    for (TablePermission perm : TablePermission.values()) {
      log.debug("Verifying the " + perm + " permission");
      
      // test permission before and after granting it
      createTestTable(c);
      testMissingTablePermission(c, test_user_conn, perm);
      c.securityOperations().grantTablePermission(TEST_USER, TEST_TABLE, perm);
      verifyHasOnlyTheseTablePermissions(c, TEST_USER, TEST_TABLE, perm);
      testGrantedTablePermission(c, test_user_conn, perm);
      
      createTestTable(c);
      c.securityOperations().revokeTablePermission(TEST_USER, TEST_TABLE, perm);
      verifyHasNoTablePermissions(c, TEST_USER, TEST_TABLE, perm);
    }
  }
  
  private void createTestTable(Connector c) throws Exception, MutationsRejectedException {
    if (!c.tableOperations().exists(TEST_TABLE)) {
      // create the test table
      c.tableOperations().create(TEST_TABLE);
      // put in some initial data
      BatchWriter writer = c.createBatchWriter(TEST_TABLE, new BatchWriterConfig());
      Mutation m = new Mutation(new Text("row"));
      m.put(new Text("cf"), new Text("cq"), new Value("val".getBytes()));
      writer.addMutation(m);
      writer.close();
      
      // verify proper permissions for creator and test user
      verifyHasOnlyTheseTablePermissions(c, c.whoami(), TEST_TABLE, TablePermission.values());
      verifyHasNoTablePermissions(c, TEST_USER, TEST_TABLE, TablePermission.values());
      
    }
  }
  
  private static void testMissingTablePermission(Connector root_conn, Connector test_user_conn, TablePermission perm) throws Exception {
    Scanner scanner;
    BatchWriter writer;
    Mutation m;
    log.debug("Confirming that the lack of the " + perm + " permission properly restricts the user");
    
    // test permission prior to granting it
    switch (perm) {
      case READ:
        try {
          scanner = test_user_conn.createScanner(TEST_TABLE, Authorizations.EMPTY);
          int i = 0;
          for (Entry<Key,Value> entry : scanner)
            i += 1 + entry.getKey().getRowData().length();
          if (i != 0)
            throw new IllegalStateException("Should NOT be able to read from the table");
        } catch (RuntimeException e) {
          AccumuloSecurityException se = (AccumuloSecurityException) e.getCause();
          if (se.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw se;
        }
        break;
      case WRITE:
        try {
          writer = test_user_conn.createBatchWriter(TEST_TABLE, new BatchWriterConfig());
          m = new Mutation(new Text("row"));
          m.put(new Text("a"), new Text("b"), new Value("c".getBytes()));
          writer.addMutation(m);
          try {
            writer.close();
          } catch (MutationsRejectedException e1) {
            if (e1.getAuthorizationFailuresMap().size() > 0)
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
        Map<String,Set<Text>> groups = new HashMap<String,Set<Text>>();
        groups.put("tgroup", new HashSet<Text>(Arrays.asList(new Text("t1"), new Text("t2"))));
        try {
          test_user_conn.tableOperations().setLocalityGroups(TEST_TABLE, groups);
          throw new IllegalStateException("User should not be able to set locality groups");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        break;
      case DROP_TABLE:
        try {
          test_user_conn.tableOperations().delete(TEST_TABLE);
          throw new IllegalStateException("User should not be able delete the table");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        break;
      case GRANT:
        try {
          test_user_conn.securityOperations().grantTablePermission("root", TEST_TABLE, TablePermission.GRANT);
          throw new IllegalStateException("User should not be able grant permissions");
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() != SecurityErrorCode.PERMISSION_DENIED)
            throw e;
        }
        break;
      default:
        throw new IllegalArgumentException("Unrecognized table Permission: " + perm);
    }
  }
  
  private static void testGrantedTablePermission(Connector root_conn, Connector test_user_conn, TablePermission perm) throws AccumuloException,
      TableExistsException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException {
    Scanner scanner;
    BatchWriter writer;
    Mutation m;
    log.debug("Confirming that the presence of the " + perm + " permission properly permits the user");
    
    // test permission after granting it
    switch (perm) {
      case READ:
        scanner = test_user_conn.createScanner(TEST_TABLE, Authorizations.EMPTY);
        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        while (iter.hasNext())
          iter.next();
        break;
      case WRITE:
        writer = test_user_conn.createBatchWriter(TEST_TABLE, new BatchWriterConfig());
        m = new Mutation(new Text("row"));
        m.put(new Text("a"), new Text("b"), new Value("c".getBytes()));
        writer.addMutation(m);
        writer.close();
        break;
      case BULK_IMPORT:
        // test for bulk import permission would go here
        break;
      case ALTER_TABLE:
        Map<String,Set<Text>> groups = new HashMap<String,Set<Text>>();
        groups.put("tgroup", new HashSet<Text>(Arrays.asList(new Text("t1"), new Text("t2"))));
        break;
      case DROP_TABLE:
        test_user_conn.tableOperations().delete(TEST_TABLE);
        break;
      case GRANT:
        test_user_conn.securityOperations().grantTablePermission("root", TEST_TABLE, TablePermission.GRANT);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized table Permission: " + perm);
    }
  }
  
  private static void verifyHasOnlyTheseTablePermissions(Connector root_conn, String user, String table, TablePermission... perms) throws AccumuloException,
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
  
  private static void verifyHasNoTablePermissions(Connector root_conn, String user, String table, TablePermission... perms) throws AccumuloException,
      AccumuloSecurityException {
    for (TablePermission p : perms)
      if (root_conn.securityOperations().hasTablePermission(user, table, p))
        throw new IllegalStateException(user + " SHOULD NOT have table permission " + p + " for table " + table);
  }
}
