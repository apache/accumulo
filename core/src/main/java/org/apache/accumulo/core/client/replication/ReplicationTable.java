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
package org.apache.accumulo.core.client.replication;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;

/**
 * 
 */
public class ReplicationTable {
  public static final String NAME = "replication";

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

  public static BatchScanner getBatchScanner(Connector conn, int queryThreads) throws TableNotFoundException {
    return conn.createBatchScanner(NAME, new Authorizations(), queryThreads);
  }

  public static boolean exists(Connector conn) {
    return exists(conn.tableOperations());
  }

  public static boolean exists(TableOperations tops) {
    return tops.exists(NAME);
  }
}
