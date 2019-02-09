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

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;

public class VolumeChooserEnvironment {

  /**
   * A scope the volume chooser environment; a TABLE scope should be accompanied by a tableId.
   *
   * @since 2.0.0
   */
  public static enum ChooserScope {
    DEFAULT, TABLE, INIT, LOGGER
  }

  private final ServerContext context;
  private final ChooserScope scope;
  private final TableId tableId;
  private final Text endRow;

  public VolumeChooserEnvironment(ChooserScope scope, ServerContext context) {
    this.context = context;
    this.scope = Objects.requireNonNull(scope);
    this.tableId = null;
    this.endRow = null;
  }

  public VolumeChooserEnvironment(TableId tableId, Text endRow, ServerContext context) {
    this.context = context;
    this.scope = ChooserScope.TABLE;
    this.tableId = Objects.requireNonNull(tableId);
    this.endRow = endRow;

  }

  /**
   * The end row of the tablet for which a volume is being chosen. Only call this when the scope is
   * TABLE
   *
   * @since 2.0.0
   */
  public Text getEndRow() {
    if (scope != ChooserScope.TABLE)
      throw new IllegalStateException("Can only request end row for tables, not for " + scope);
    return endRow;
  }

  public TableId getTableId() {
    return tableId;
  }

  public ChooserScope getScope() {
    return this.scope;
  }

  public ServerContext getServerContext() {
    if (context == null) {
      throw new IllegalStateException("Requested ServerContext from " + getClass().getSimpleName()
          + " that was created without it");
    }
    return context;
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
