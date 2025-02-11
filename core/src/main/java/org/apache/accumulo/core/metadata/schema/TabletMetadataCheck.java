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
import java.util.Set;

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

  boolean canUpdate(TabletMetadata tabletMetadata);

  /**
   * Determines what tablet metadata columns are read on the server side. Return
   * {@link #ALL_COLUMNS} to read all of a tablets metadata.
   */
  default Set<TabletMetadata.ColumnType> columnsToRead() {
    return ALL_COLUMNS;
  }
}
