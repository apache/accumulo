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
package org.apache.accumulo.server.replication;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.StatusFormatter;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class ReplicationTable extends org.apache.accumulo.core.client.replication.ReplicationTable {
  private static final Logger log = LoggerFactory.getLogger(ReplicationTable.class);

  public static final String COMBINER_NAME = "statuscombiner";

  public static final String STATUS_LG_NAME = StatusSection.NAME.toString();
  public static final Set<Text> STATUS_LG_COLFAMS = Collections.singleton(StatusSection.NAME);
  public static final String WORK_LG_NAME = WorkSection.NAME.toString();
  public static final Set<Text> WORK_LG_COLFAMS = Collections.singleton(WorkSection.NAME);
  public static final Map<String,Set<Text>> LOCALITY_GROUPS = ImmutableMap.of(STATUS_LG_NAME, STATUS_LG_COLFAMS, WORK_LG_NAME, WORK_LG_COLFAMS);
  public static final String STATUS_FORMATTER_CLASS_NAME = StatusFormatter.class.getName();

  public static synchronized void create(Connector conn) {
    TableOperations tops = conn.tableOperations();
    if (tops.exists(NAME)) {
      if (configureReplicationTable(conn)) {
        return;
      }
    }

    for (int i = 0; i < 5; i++) {
      try {
        if (!tops.exists(NAME)) {
          tops.create(NAME, false);
        }
        break;
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("Failed to create replication table", e);
      } catch (TableExistsException e) {
        // Shouldn't happen unless someone else made the table
      }
      log.error("Retrying table creation in 1 second...");
      UtilWaitThread.sleep(1000);
    }

    for (int i = 0; i < 5; i++) {
      if (configureReplicationTable(conn)) {
        return;
      }

      log.error("Failed to configure the replication table, retying...");
      UtilWaitThread.sleep(1000);
    }

    throw new RuntimeException("Could not configure replication table");
  }

  /**
   * Attempts to configure the replication table, will return false if it fails
   * 
   * @param conn
   *          Connector for the instance
   * @return True if the replication table is properly configured
   */
  protected static synchronized boolean configureReplicationTable(Connector conn) {
    try {
      conn.securityOperations().grantTablePermission("root", NAME, TablePermission.READ);
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.warn("Could not grant root user read access to replication table", e);
      // Should this be fatal? It's only for convenience, all r/w is done by !SYSTEM
    }

    TableOperations tops = conn.tableOperations();
    Map<String,EnumSet<IteratorScope>> iterators = null;
    try {
      iterators = tops.listIterators(NAME);
    } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
      log.error("Could not fetch iterators for " + NAME, e);
      return false;
    }

    if (!iterators.containsKey(COMBINER_NAME)) {
      // Set our combiner and combine all columns
      IteratorSetting setting = new IteratorSetting(30, COMBINER_NAME, StatusCombiner.class);
      Combiner.setColumns(setting, Arrays.asList(new Column(StatusSection.NAME), new Column(WorkSection.NAME)));
      try {
        tops.attachIterator(NAME, setting);
      } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
        log.error("Could not set StatusCombiner on replication table", e);
        return false;
      }
    }

    Map<String,Set<Text>> localityGroups;
    try {
      localityGroups = tops.getLocalityGroups(NAME);
    } catch (TableNotFoundException | AccumuloException e) {
      log.error("Could not fetch locality groups", e);
      return false;
    }

    Set<Text> statusColfams = localityGroups.get(STATUS_LG_NAME), workColfams = localityGroups.get(WORK_LG_NAME);
    if (null == statusColfams || null == workColfams) {
      try {
        tops.setLocalityGroups(NAME, LOCALITY_GROUPS);
      } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
        log.error("Could not set locality groups on replication table", e);
        return false;
      }
    }

    // Make sure the StatusFormatter is set on the metadata table
    Iterable<Entry<String,String>> properties;
    try {
      properties = tops.getProperties(NAME);
    } catch (AccumuloException | TableNotFoundException e) {
      log.error("Could not fetch table properties on replication table", e);
      return false;
    }

    boolean formatterConfigured = false;
    for (Entry<String,String> property : properties) {
      if (Property.TABLE_FORMATTER_CLASS.getKey().equals(property.getKey())) {
        if (!STATUS_FORMATTER_CLASS_NAME.equals(property.getValue())) {
          log.info("Changing formatter for {} table from {} to {}", NAME, property.getValue(), STATUS_FORMATTER_CLASS_NAME);
          try {
            tops.setProperty(NAME, Property.TABLE_FORMATTER_CLASS.getKey(), STATUS_FORMATTER_CLASS_NAME);
          } catch (AccumuloException | AccumuloSecurityException e) {
            log.error("Could not set formatter on replication table", e);
            return false;
          }
        }

        formatterConfigured = true;

        // Don't need to keep iterating over the properties after we found the one we were looking for
        break;
      }
    }

    if (!formatterConfigured) {
      try {
        tops.setProperty(NAME, Property.TABLE_FORMATTER_CLASS.getKey(), STATUS_FORMATTER_CLASS_NAME);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("Could not set formatter on replication table", e);
        return false;
      }
    }

    log.debug("Successfully configured replication table");

    return true;
  }
}
