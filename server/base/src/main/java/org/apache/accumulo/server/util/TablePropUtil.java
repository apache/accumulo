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
package org.apache.accumulo.server.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropCacheKey;

public class TablePropUtil implements PropUtil {

  private TablePropUtil() {}

  public static TablePropUtil factory() {
    return new TablePropUtil();
  }

  /**
   * Helper method to set provided properties for the provided table.
   *
   * @throws IllegalStateException
   *           if an underlying exception (KeeperException, InterruptException) or other failure to
   *           read properties from the cache / backend store
   */
  @Override
  public void setProperties(ServerContext context, AbstractId<?> tableId,
      Map<String,String> props) {
    Map<String,String> tempProps = new HashMap<>(props);
    // TODO reconcile with NamespacePropUtil see https://github.com/apache/accumulo/issues/2633
    tempProps.entrySet().removeIf(e -> !Property.isTablePropertyValid(e.getKey(), e.getValue()));

    context.getPropStore().putAll(PropCacheKey.forTable(context, (TableId) tableId), props);
  }

  @Override
  public void removeProperties(final ServerContext context, final AbstractId<?> tableId,
      Collection<String> propertyNames) {
    context.getPropStore().removeProperties(PropCacheKey.forTable(context, (TableId) tableId),
        propertyNames);
  }

}
