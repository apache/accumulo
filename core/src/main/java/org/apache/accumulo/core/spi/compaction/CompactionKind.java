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
package org.apache.accumulo.core.spi.compaction;

import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;

/**
 * @since 2.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */
public enum CompactionKind {
  /**
   * A system initiated routine compaction.
   */
  SYSTEM,
  /**
   * Set of files selected by a {@link CompactionSelector} configured for a table.
   *
   * @deprecated since 3.1. Use of selector compactions should be replaced with user compactions
   *             initiated via
   *             {@link org.apache.accumulo.core.client.admin.TableOperations#compact(String, CompactionConfig)}.
   *             Everything that can be done with selector compactions can also be done with user
   *             compactions. User compactions offer more control over when compactions run, the
   *             range of data compacted, and the ability to cancel. Selector compactions offer none
   *             of these features and were deprecated in favor of only offering user compactions.
   */
  @Deprecated(since = "3.1")
  SELECTOR,
  /**
   * A user initiated a one time compaction using an Accumulo client.
   */
  USER
}
