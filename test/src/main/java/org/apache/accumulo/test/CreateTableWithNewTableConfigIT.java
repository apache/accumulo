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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

/**
 *
 */
public class CreateTableWithNewTableConfigIT extends SharedMiniClusterBase {
  static private final Logger log = LoggerFactory.getLogger(CreateTableWithNewTableConfigIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 30;
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  public int numProperties(Connector connector, String tableName) throws AccumuloException, TableNotFoundException {
    return Iterators.size(connector.tableOperations().getProperties(tableName).iterator());
  }

  public int compareProperties(Connector connector, String tableNameOrig, String tableName, String changedProp) throws AccumuloException,
      TableNotFoundException {
    boolean inNew = false;
    int countOrig = 0;
    for (Entry<String,String> orig : connector.tableOperations().getProperties(tableNameOrig)) {
      countOrig++;
      for (Entry<String,String> entry : connector.tableOperations().getProperties(tableName)) {
        if (entry.equals(orig)) {
          inNew = true;
          break;
        } else if (entry.getKey().equals(orig.getKey()) && !entry.getKey().equals(changedProp))
          Assert.fail("Property " + orig.getKey() + " has different value than deprecated method");
      }
      if (!inNew)
        Assert.fail("Original property missing after using the new create method");
    }
    return countOrig;
  }

  public boolean checkTimeType(Connector connector, String tableName, TimeType expectedTimeType) throws TableNotFoundException {
    final Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    String tableID = connector.tableOperations().tableIdMap().get(tableName) + "<";
    for (Entry<Key,Value> entry : scanner) {
      Key k = entry.getKey();

      if (k.getRow().toString().equals(tableID) && k.getColumnQualifier().toString().equals(ServerColumnFamily.TIME_COLUMN.getColumnQualifier().toString())) {
        if (expectedTimeType == TimeType.MILLIS && entry.getValue().toString().charAt(0) == 'M')
          return true;
        if (expectedTimeType == TimeType.LOGICAL && entry.getValue().toString().charAt(0) == 'L')
          return true;
      }
    }
    return false;
  }

  @SuppressWarnings("deprecation")
  @Test
  public void tableNameOnly() throws Exception {
    log.info("Starting tableNameOnly");

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, new NewTableConfiguration());

    String tableNameOrig = "original";
    connector.tableOperations().create(tableNameOrig, true);

    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, null);

    Assert.assertEquals("Extra properties using the new create method", countOrig, countNew);
    Assert.assertTrue("Wrong TimeType", checkTimeType(connector, tableName, TimeType.MILLIS));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void tableNameAndLimitVersion() throws Exception {
    log.info("Starting tableNameAndLimitVersion");

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    boolean limitVersion = false;
    connector.tableOperations().create(tableName, new NewTableConfiguration().withoutDefaultIterators());

    String tableNameOrig = "originalWithLimitVersion";
    connector.tableOperations().create(tableNameOrig, limitVersion);

    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, null);

    Assert.assertEquals("Extra properties using the new create method", countOrig, countNew);
    Assert.assertTrue("Wrong TimeType", checkTimeType(connector, tableName, TimeType.MILLIS));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void tableNameLimitVersionAndTimeType() throws Exception {
    log.info("Starting tableNameLimitVersionAndTimeType");

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    boolean limitVersion = false;
    TimeType tt = TimeType.LOGICAL;
    connector.tableOperations().create(tableName, new NewTableConfiguration().withoutDefaultIterators().setTimeType(tt));

    String tableNameOrig = "originalWithLimitVersionAndTimeType";
    connector.tableOperations().create(tableNameOrig, limitVersion, tt);

    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, null);

    Assert.assertEquals("Extra properties using the new create method", countOrig, countNew);
    Assert.assertTrue("Wrong TimeType", checkTimeType(connector, tableName, tt));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void addCustomPropAndChangeExisting() throws Exception {
    log.info("Starting addCustomPropAndChangeExisting");

    // Create and populate initial properties map for creating table 1
    Map<String,String> properties = new HashMap<>();
    String propertyName = Property.TABLE_SPLIT_THRESHOLD.getKey();
    String volume = "10K";
    properties.put(propertyName, volume);

    String propertyName2 = "table.custom.testProp";
    String volume2 = "Test property";
    properties.put(propertyName2, volume2);

    // Create a table with the initial properties
    Connector connector = getConnector();
    String tableName = getUniqueNames(2)[0];
    connector.tableOperations().create(tableName, new NewTableConfiguration().setProperties(properties));

    String tableNameOrig = "originalWithTableName";
    connector.tableOperations().create(tableNameOrig, true);

    int countNew = numProperties(connector, tableName);
    int countOrig = compareProperties(connector, tableNameOrig, tableName, propertyName);

    for (Entry<String,String> entry : connector.tableOperations().getProperties(tableName)) {
      if (entry.getKey().equals(Property.TABLE_SPLIT_THRESHOLD.getKey()))
        Assert.assertTrue("TABLE_SPLIT_THRESHOLD has been changed", entry.getValue().equals("10K"));
      if (entry.getKey().equals("table.custom.testProp"))
        Assert.assertTrue("table.custom.testProp has been changed", entry.getValue().equals("Test property"));
    }

    Assert.assertEquals("Extra properties using the new create method", countOrig + 1, countNew);
    Assert.assertTrue("Wrong TimeType", checkTimeType(connector, tableName, TimeType.MILLIS));

  }
}
