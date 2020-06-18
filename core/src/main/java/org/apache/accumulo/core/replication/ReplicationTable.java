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
package org.apache.accumulo.core.replication;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationTable {
  private static final Logger log = LoggerFactory.getLogger(ReplicationTable.class);

  public static final TableId ID = TableId.of("+rep");
  public static final String NAME = Namespace.ACCUMULO.name() + ".replication";

  public static final String COMBINER_NAME = "statuscombiner";

  public static final String STATUS_LG_NAME = StatusSection.NAME.toString();
  public static final Set<Text> STATUS_LG_COLFAMS = Collections.singleton(StatusSection.NAME);
  public static final String WORK_LG_NAME = WorkSection.NAME.toString();
  public static final Set<Text> WORK_LG_COLFAMS = Collections.singleton(WorkSection.NAME);
  public static final Map<String,Set<Text>> LOCALITY_GROUPS =
      Map.of(STATUS_LG_NAME, STATUS_LG_COLFAMS, WORK_LG_NAME, WORK_LG_COLFAMS);

  public static Scanner getScanner(AccumuloClient client) throws ReplicationTableOfflineException {
    try {
      return client.createScanner(NAME, Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      throw new AssertionError(NAME + " should exist, but doesn't.");
    } catch (TableOfflineException e) {
      throw new ReplicationTableOfflineException(e);
    }
  }

  public static BatchWriter getBatchWriter(AccumuloClient client)
      throws ReplicationTableOfflineException {
    try {
      return client.createBatchWriter(NAME, new BatchWriterConfig());
    } catch (TableNotFoundException e) {
      throw new AssertionError(NAME + " should exist, but doesn't.");
    } catch (TableOfflineException e) {
      throw new ReplicationTableOfflineException(e);
    }
  }

  public static BatchScanner getBatchScanner(AccumuloClient client, int queryThreads)
      throws ReplicationTableOfflineException {
    try {
      return client.createBatchScanner(NAME, Authorizations.EMPTY, queryThreads);
    } catch (TableNotFoundException e) {
      throw new AssertionError(NAME + " should exist, but doesn't.");
    } catch (TableOfflineException e) {
      throw new ReplicationTableOfflineException(e);
    }
  }

  public static boolean isOnline(AccumuloClient client) {
    return Tables.getTableState((ClientContext) client, ID) == TableState.ONLINE;
  }

  public static void setOnline(AccumuloClient client)
      throws AccumuloSecurityException, AccumuloException {
    try {
      log.info("Bringing replication table online");
      client.tableOperations().online(NAME, true);
    } catch (TableNotFoundException e) {
      throw new AssertionError(NAME + " should exist, but doesn't.");
    }
  }

}
