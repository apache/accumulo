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
package org.apache.accumulo.core.metadata;

import java.util.SortedMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.ArgumentChecker;

/**
 * Provides a consolidated API for handling table metadata
 */
public abstract class MetadataServicer {

  public static MetadataServicer forTableName(Instance instance, Credentials credentials, String tableName) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName);
    Connector conn = instance.getConnector(credentials.getPrincipal(), credentials.getToken());
    return forTableId(instance, credentials, conn.tableOperations().tableIdMap().get(tableName));
  }

  public static MetadataServicer forTableId(Instance instance, Credentials credentials, String tableId) {
    ArgumentChecker.notNull(tableId);
    if (RootTable.ID.equals(tableId))
      return new ServicerForRootTable(instance, credentials);
    else if (MetadataTable.ID.equals(tableId))
      return new ServicerForMetadataTable(instance, credentials);
    else
      return new ServicerForUserTables(instance, credentials, tableId);
  }

  /**
   *
   * @return the table id of the table currently being serviced
   */
  public abstract String getServicedTableId();

  /**
   * Populate the provided data structure with the known tablets for the table being serviced
   *
   * @param tablets
   *          A mapping of all known tablets to their location (if available, null otherwise)
   */
  public abstract void getTabletLocations(SortedMap<KeyExtent,String> tablets) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

}
