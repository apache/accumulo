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
package org.apache.accumulo.core.spi.scan;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.TabletId;

/**
 * This interface exists so that is easier to evolve what is passed to
 * {@link ScanServerSelector#determineActions(ScanServerSelectorParameters)} without having to make
 * breaking changes.
 *
 * @since 2.1.0
 */
public interface ScanServerSelectorParameters {

  /**
   * @return the set of tablets to be scanned
   */
  Collection<TabletId> getTablets();

  /**
   * @return scan attempt information for the tablet
   */
  Collection<? extends ScanServerScanAttempt> getAttempts(TabletId tabletId);

  /**
   * @return any hints set on a scanner using
   *         {@link org.apache.accumulo.core.client.ScannerBase#setExecutionHints(Map)}. If none
   *         were set, an empty map is returned.
   */
  Map<String,String> getHints();
}
