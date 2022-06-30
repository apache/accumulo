/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.util;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.TablePropKey;

public class TablePropUtil extends PropUtil<TableId> {

  public TablePropUtil(ServerContext context) {
    super(context);
  }

  public void setProperties(TableId tableId, Map<String,String> properties) {
    setProperties(tableId, properties, TablePropKey.of(context, tableId));
  }

  public void removeProperties(TableId tableId, Collection<String> propertyNames) {
    removeProperties(TablePropKey.of(context, tableId), propertyNames);
  }

}
