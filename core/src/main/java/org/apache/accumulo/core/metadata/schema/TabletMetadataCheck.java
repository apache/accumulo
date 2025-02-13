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
package org.apache.accumulo.core.metadata.schema;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;

/**
 * This interface facilitates atomic checks of tablet metadata prior to updating tablet metadata.
 * The way it is intended to be used is the following.
 * <ol>
 * <li>On the client side a TabletMetadataCheck object is created and passed to Ample</li>
 * <li>Ample uses Gson to serialize the object, so it must not reference anything that can not
 * serialize. Also it should not reference big things like server context or the Manager, it should
 * only reference a small amount of data needed for the check.
 * <li>On the tablet server side, as part of conditional mutation processing, this class is
 * recreated and the {@link #canUpdate(TabletMetadata)} method is called and if it returns true the
 * conditional mutation will go through.</li>
 * </ol>
 *
 * <p>
 * Implementations are expected to have a no arg constructor.
 * </p>
 *
 */
public interface TabletMetadataCheck {

  Set<TabletMetadata.ColumnType> ALL_COLUMNS =
      Collections.unmodifiableSet(EnumSet.allOf(TabletMetadata.ColumnType.class));

  ResolvedColumns ALL_RESOLVED_COLUMNS = new ResolvedColumns(ALL_COLUMNS);

  boolean canUpdate(TabletMetadata tabletMetadata);

  /**
   * Determines what tablet metadata columns/families are read on the server side. Return
   * {@link #ALL_RESOLVED_COLUMNS} to read all of a tablets metadata. If all columns are included,
   * the families set will be empty which means read all families.
   */
  default ResolvedColumns columnsToRead() {
    return ALL_RESOLVED_COLUMNS;
  }

  class ResolvedColumns {
    private final Set<TabletMetadata.ColumnType> columns;
    private final Set<ByteSequence> families;

    public ResolvedColumns(Set<ColumnType> columns) {
      this.columns = Objects.requireNonNull(columns);
      this.families = columns.equals(ALL_COLUMNS) ? Set.of() : ColumnType.resolveFamilies(columns);
    }

    public Set<ColumnType> getColumns() {
      return columns;
    }

    public Set<ByteSequence> getFamilies() {
      return families;
    }
  }

}
