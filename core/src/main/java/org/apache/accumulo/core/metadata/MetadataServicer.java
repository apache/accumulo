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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.SortedMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.impl.KeyExtent;

/**
 * Provides a consolidated API for handling table metadata
 */
public abstract class MetadataServicer {

  public static MetadataServicer forTableName(ClientContext context, String tableName) throws AccumuloException, AccumuloSecurityException {
    checkArgument(tableName != null, "tableName is null");
    return forTableId(context, Table.ID.of(context.getConnector().tableOperations().tableIdMap().get(tableName)));
  }

  public static MetadataServicer forTableId(ClientContext context, Table.ID tableId) {
    checkArgument(tableId != null, "tableId is null");
    if (RootTable.ID.equals(tableId))
      return new ServicerForRootTable(context);
    else if (MetadataTable.ID.equals(tableId))
      return new ServicerForMetadataTable(context);
    else
      return new ServicerForUserTables(context, tableId);
  }

  /**
   *
   * @return the table id of the table currently being serviced
   */
  public abstract Table.ID getServicedTableId();

  /**
   * Populate the provided data structure with the known tablets for the table being serviced
   *
   * @param tablets
   *          A mapping of all known tablets to their location (if available, null otherwise)
   */
  public abstract void getTabletLocations(SortedMap<KeyExtent,String> tablets) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

}
