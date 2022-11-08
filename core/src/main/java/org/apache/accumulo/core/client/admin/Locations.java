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
package org.apache.accumulo.core.client.admin;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TabletId;

/**
 * A snapshot of metadata information about where a specified set of ranges are located returned by
 * {@link TableOperations#locate(String, java.util.Collection)}
 *
 * @since 1.8.0
 */
public interface Locations {

  /**
   * For all of the ranges passed to {@link TableOperations#locate(String, java.util.Collection)},
   * return a map of the tablets each range overlaps.
   */
  Map<Range,List<TabletId>> groupByRange();

  /**
   * For all of the ranges passed to {@link TableOperations#locate(String, java.util.Collection)},
   * return a map of the ranges each tablet overlaps.
   */
  Map<TabletId,List<Range>> groupByTablet();

  /**
   * For any {@link TabletId} known to this object, the method will return the tablet server
   * location for that tablet.
   *
   * @return A tablet server location in the form of {@code <host>:<port>}
   */
  String getTabletLocation(TabletId tabletId);
}
