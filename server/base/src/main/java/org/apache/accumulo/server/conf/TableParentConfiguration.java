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
package org.apache.accumulo.server.conf;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.ServerInfo;

/**
 * Used by TableConfiguration to dynamically get the NamespaceConfiguration if the namespace changes
 */
public class TableParentConfiguration extends NamespaceConfiguration {

  private Table.ID tableId;
  private AccumuloServerContext context;

  public TableParentConfiguration(Table.ID tableId, ServerInfo info, AccumuloConfiguration parent) {
    super(null, info, parent);
    this.tableId = tableId;
    this.namespaceId = getNamespaceId();
    this.context = new AccumuloServerContext(info);
  }

  @Override
  protected Namespace.ID getNamespaceId() {
    try {
      return Tables.getNamespaceId(context, tableId);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
