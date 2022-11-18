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
package org.apache.accumulo.server.fs;

import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * Volume chooser authors should avoid using this class when testing their volume chooser. The
 * constructors for this class may change at any time. For testing purposes mocking the interface
 * {@code VolumeChooserEnvironment} should result in more stable code over time than using this
 * class.
 */
@SuppressWarnings("deprecation")
public class VolumeChooserEnvironmentImpl implements VolumeChooserEnvironment {

  private final ServerContext context;
  private final Scope scope;
  private final Optional<TableId> tableId;
  private final Text endRow;
  private final ServiceEnvironment senv;

  public VolumeChooserEnvironmentImpl(Scope scope, ServerContext context) {
    this.context = context;
    this.scope = Objects.requireNonNull(scope);
    this.tableId = Optional.empty();
    this.endRow = null;
    this.senv = new ServiceEnvironmentImpl(context);
  }

  public VolumeChooserEnvironmentImpl(TableId tableId, Text endRow, ServerContext context) {
    this.context = context;
    this.scope = Scope.TABLE;
    this.tableId = Optional.of(tableId);
    this.endRow = endRow;
    this.senv = new ServiceEnvironmentImpl(context);
  }

  public VolumeChooserEnvironmentImpl(Scope scope, TableId tableId, Text endRow,
      ServerContext context) {
    this.context = context;
    this.scope = Objects.requireNonNull(scope);
    this.tableId = Optional.of(tableId);
    this.endRow = endRow;
    this.senv = new ServiceEnvironmentImpl(context);
  }

  /**
   * The end row of the tablet for which a volume is being chosen. Only call this when the scope is
   * TABLE
   *
   * @since 2.0.0
   */
  @Override
  public Text getEndRow() {
    if (scope != Scope.TABLE && scope != Scope.INIT) {
      throw new IllegalStateException("Can only request end row for tables, not for " + scope);
    }
    return endRow;
  }

  @Override
  public Optional<TableId> getTable() {
    return tableId;
  }

  @Override
  public boolean hasTableId() {
    return tableId.isPresent();
  }

  @Override
  public TableId getTableId() {
    return tableId.get();
  }

  @Override
  public Scope getChooserScope() {
    return this.scope;
  }

  @Override
  public ServiceEnvironment getServiceEnv() {
    return senv;
  }

  @Override
  public FileSystem getFileSystem(String option) {
    return context.getVolumeManager().getFileSystemByPath(new Path(option));
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || !(obj instanceof VolumeChooserEnvironmentImpl)) {
      return false;
    }
    VolumeChooserEnvironmentImpl other = (VolumeChooserEnvironmentImpl) obj;
    return getChooserScope() == other.getChooserScope()
        && Objects.equals(getTableId(), other.getTableId());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(scope) * 31 + Objects.hashCode(tableId);
  }
}
