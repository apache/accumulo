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
package org.apache.accumulo.core.fate;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.manager.thrift.TFateInstanceType;
import org.apache.accumulo.core.metadata.SystemTables;

public enum FateInstanceType {
  META, USER;

  public static FateInstanceType fromNamespaceOrTableName(String tableOrNamespaceName) {
    return tableOrNamespaceName.startsWith(Namespace.ACCUMULO.name()) ? FateInstanceType.META
        : FateInstanceType.USER;
  }

  public TFateInstanceType toThrift() {
      return switch (this) {
          case USER -> TFateInstanceType.USER;
          case META -> TFateInstanceType.META;
          default -> throw new IllegalStateException("Unknown FateInstance type " + this);
      };
  }

  public static FateInstanceType fromThrift(TFateInstanceType tfit) {
      return switch (tfit) {
          case USER -> FateInstanceType.USER;
          case META -> FateInstanceType.META;
          default -> throw new IllegalStateException("Unknown type " + tfit);
      };
  }

  public static FateInstanceType fromTableId(TableId tableId) {
    return SystemTables.containsTableId(tableId) ? META : USER;
  }
}
