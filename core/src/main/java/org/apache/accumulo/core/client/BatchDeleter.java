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
package org.apache.accumulo.core.client;

import java.util.Collection;

import org.apache.accumulo.core.data.Range;

/**
 * Implementations of BatchDeleter support efficient deletion of ranges in accumulo.
 *
 */

public interface BatchDeleter extends ScannerBase {
  /**
   * Deletes the ranges specified by {@link #setRanges}.
   *
   * @throws MutationsRejectedException
   *           this can be thrown when deletion mutations fail
   * @throws TableNotFoundException
   *           when the table does not exist
   */
  void delete() throws MutationsRejectedException, TableNotFoundException;

  /**
   * Allows deleting multiple ranges efficiently.
   *
   * @param ranges
   *          specifies the non-overlapping ranges to query
   */
  void setRanges(Collection<Range> ranges);

  /**
   * Releases any resources.
   */
  void close();
}
