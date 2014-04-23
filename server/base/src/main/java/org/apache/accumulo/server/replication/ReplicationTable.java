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

import java.util.EnumSet;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.log4j.Logger;

public class ReplicationTable {
  private static final Logger log = Logger.getLogger(ReplicationTable.class);

  public static final String NAME = "replication";
  public static final String COMBINER_NAME = "combiner";

  public static synchronized void create(TableOperations tops) {
    if (tops.exists(NAME)) {
      if (configure(tops)) {
        return;
      }
    }

    for (int i = 0; i < 5; i++) {
      try {
        tops.create(NAME);
        configure(tops);
        return;
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("Failed to create replication table", e);
      } catch (TableExistsException e) {
        // Shouldn't happen unless FATE is broken
        configure(tops);
        return;
      }
      log.error("Retrying table creation in 1 second...");
      UtilWaitThread.sleep(1000);
    }
  }

  /**
   * Attempts to configure the replication table, will return false if it fails
   * @param tops TableOperations for the instance
   * @return True if the replication table is properly configured
   */
  protected static synchronized boolean configure(TableOperations tops) {
    Map<String,EnumSet<IteratorScope>> iterators = null;
    try {
      iterators = tops.listIterators(NAME);
    } catch (AccumuloSecurityException|AccumuloException|TableNotFoundException e) {
      log.error("Could not fetch iterators for " + NAME, e);
      return false;
    }

    if (!iterators.containsKey(COMBINER_NAME)) {
      IteratorSetting setting = new IteratorSetting(50, COMBINER_NAME, StatusCombiner.class);
      try {
        tops.attachIterator(NAME, setting);
      } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
        log.error("Could not set StatusCombiner on replication table", e);
        return false;
      }
    }

    return true;
  }

  public static Scanner getScanner(Connector conn, Authorizations auths) throws TableNotFoundException {
    return conn.createScanner(NAME, auths);
  }

  public static Scanner getScanner(Connector conn) throws TableNotFoundException {
    return getScanner(conn, new Authorizations());
  }

  public static BatchWriter getBatchWriter(Connector conn) throws TableNotFoundException {
    return getBatchWriter(conn, new BatchWriterConfig());
  }

  public static BatchWriter getBatchWriter(Connector conn, BatchWriterConfig config) throws TableNotFoundException {
    return conn.createBatchWriter(NAME, config);
  }
}
