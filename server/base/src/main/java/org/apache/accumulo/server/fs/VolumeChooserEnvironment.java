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
package org.apache.accumulo.server.fs;

import java.util.Objects;

import org.apache.accumulo.core.client.impl.Table;

public class VolumeChooserEnvironment {

  /**
   * A scope the volume chooser environment; a TABLE scope should be accompanied by a tableId.
   *
   * @since 2.0.0
   */
  public static enum ChooserScope {
    DEFAULT, TABLE, INIT, LOGGER
  }

  private final ChooserScope scope;
  private final Table.ID tableId;

  public VolumeChooserEnvironment(ChooserScope scope) {
    this.scope = Objects.requireNonNull(scope);
    this.tableId = null;
  }

  public VolumeChooserEnvironment(Table.ID tableId) {
    this.scope = ChooserScope.TABLE;
    this.tableId = Objects.requireNonNull(tableId);
  }

  public Table.ID getTableId() {
    return tableId;
  }

  public ChooserScope getScope() {
    return this.scope;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || !(obj instanceof VolumeChooserEnvironment)) {
      return false;
    }
    VolumeChooserEnvironment other = (VolumeChooserEnvironment) obj;
    return getScope() == other.getScope() && Objects.equals(getTableId(), other.getTableId());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(scope) * 31 + Objects.hashCode(tableId);
  }
}
