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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArbitraryTablePropertiesIT extends SharedMiniClusterBase {
  private static final Logger log = LoggerFactory.getLogger(ArbitraryTablePropertiesIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 30;
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterClass
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  // Test set, get, and remove arbitrary table properties on the root account
  @Test
  public void setGetRemoveTablePropertyRoot() throws Exception {
    log.debug("Starting setGetRemoveTablePropertyRoot test ------------------------");

    // make a table
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(tableName);

      // Set variables for the property name to use and the initial value
      String propertyName = "table.custom.description";
      String description1 = "Description";

      // Make sure the property name is valid
      assertTrue(Property.isValidPropertyKey(propertyName));
      // Set the property to the desired value
      client.tableOperations().setProperty(tableName, propertyName, description1);

      // Loop through properties to make sure the new property is added to the list
      int count = 0;
      for (Entry<String,String> property : client.tableOperations().getProperties(tableName)) {
        if (property.getKey().equals(propertyName) && property.getValue().equals(description1))
          count++;
      }
      assertEquals(count, 1);

      // Set the property as something different
      String description2 = "set second";
      client.tableOperations().setProperty(tableName, propertyName, description2);

      // / Loop through properties to make sure the new property is added to the list
      count = 0;
      for (Entry<String,String> property : client.tableOperations().getProperties(tableName)) {
        if (property.getKey().equals(propertyName) && property.getValue().equals(description2))
          count++;
      }
      assertEquals(count, 1);

      // Remove the property and make sure there is no longer a value associated with it
      client.tableOperations().removeProperty(tableName, propertyName);

      // / Loop through properties to make sure the new property is added to the list
      count = 0;
      for (Entry<String,String> property : client.tableOperations().getProperties(tableName)) {
        if (property.getKey().equals(propertyName))
          count++;
      }
      assertEquals(count, 0);
    }
  }

  // Tests set, get, and remove of user added arbitrary properties using a non-root account with
  // permissions to alter tables
  @Test
  public void userSetGetRemoveTablePropertyWithPermission() throws Exception {
    log.debug("Starting userSetGetRemoveTablePropertyWithPermission test ------------------------");

    // Make a test username and password
    ClusterUser user = getUser(0);
    String testUser = user.getPrincipal();
    AuthenticationToken testToken = user.getToken();

    // Create a root user and create the table
    // Create a test user and grant that user permission to alter the table
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.securityOperations().createLocalUser(testUser,
          (testToken instanceof PasswordToken ? (PasswordToken) testToken : null));
      c.tableOperations().create(tableName);
      c.securityOperations().grantTablePermission(testUser, tableName, TablePermission.ALTER_TABLE);

      // Set variables for the property name to use and the initial value
      String propertyName = "table.custom.description";
      String description1 = "Description";

      // Make sure the property name is valid
      assertTrue(Property.isValidPropertyKey(propertyName));

      // Getting a fresh token will ensure we're logged in as this user (if necessary)
      try (AccumuloClient testclient = Accumulo.newClient().from(c.properties())
          .as(testUser, user.getToken()).build()) {
        // Set the property to the desired value
        testclient.tableOperations().setProperty(tableName, propertyName, description1);

        // Loop through properties to make sure the new property is added to the list
        int count = 0;
        for (Entry<String,String> property : testclient.tableOperations()
            .getProperties(tableName)) {
          if (property.getKey().equals(propertyName) && property.getValue().equals(description1))
            count++;
        }
        assertEquals(count, 1);

        // Set the property as something different
        String description2 = "set second";
        testclient.tableOperations().setProperty(tableName, propertyName, description2);

        // / Loop through properties to make sure the new property is added to the list
        count = 0;
        for (Entry<String,String> property : testclient.tableOperations()
            .getProperties(tableName)) {
          if (property.getKey().equals(propertyName) && property.getValue().equals(description2))
            count++;
        }
        assertEquals(count, 1);

        // Remove the property and make sure there is no longer a value associated with it
        testclient.tableOperations().removeProperty(tableName, propertyName);

        // / Loop through properties to make sure the new property is added to the list
        count = 0;
        for (Entry<String,String> property : testclient.tableOperations()
            .getProperties(tableName)) {
          if (property.getKey().equals(propertyName))
            count++;
        }
        assertEquals(count, 0);
      }
    }

  }

  // Tests set and get of user added arbitrary properties using a non-root account without
  // permissions to alter tables
  @Test
  public void userSetGetTablePropertyWithoutPermission() throws Exception {
    log.debug("Starting userSetGetTablePropertyWithoutPermission test ------------------------");

    // Make a test username and password
    ClusterUser user = getUser(1);
    String testUser = user.getPrincipal();
    AuthenticationToken testToken = user.getToken();

    // Create a root user and create the table
    // Create a test user and grant that user permission to alter the table
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.securityOperations().createLocalUser(testUser,
          (testToken instanceof PasswordToken ? (PasswordToken) testToken : null));
      c.tableOperations().create(tableName);

      // Set variables for the property name to use and the initial value
      String propertyName = "table.custom.description";
      String description1 = "Description";

      // Make sure the property name is valid
      assertTrue(Property.isValidPropertyKey(propertyName));

      // Getting a fresh token will ensure we're logged in as this user (if necessary)
      try (AccumuloClient testclient = Accumulo.newClient().from(c.properties())
          .as(testUser, user.getToken()).build()) {

        // Try to set the property to the desired value.
        // If able to set it, the test fails, since permission was never granted
        try {
          testclient.tableOperations().setProperty(tableName, propertyName, description1);
          fail("Was able to set property without permissions");
        } catch (AccumuloSecurityException e) {}

        // Loop through properties to make sure the new property is not added to the list
        int count = 0;
        for (Entry<String,String> property : testclient.tableOperations()
            .getProperties(tableName)) {
          if (property.getKey().equals(propertyName))
            count++;
        }
        assertEquals(count, 0);
      }
    }
  }
}
