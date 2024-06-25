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
package org.apache.accumulo.core.metadata;

import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;

public interface ScanServerRefStore {

  /**
   * Insert ScanServer references to Tablet files
   *
   * @param scanRefs set of scan server ref table file objects
   */
  default void put(Collection<ScanServerRefTabletFile> scanRefs) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get ScanServer references to Tablet files
   *
   * @return stream of scan server references
   */
  default Stream<ScanServerRefTabletFile> list() {
    throw new UnsupportedOperationException();
  }

  /**
   * Delete the set of scan server references
   *
   * @param refsToDelete set of scan server references to delete
   */
  default void delete(Collection<ScanServerRefTabletFile> refsToDelete) {
    throw new UnsupportedOperationException();
  }

  /**
   * Delete scan server references for this server
   *
   * @param serverAddress address of server, cannot be null
   * @param serverSessionId server session id, cannot be null
   */
  default void delete(String serverAddress, UUID serverSessionId) {
    throw new UnsupportedOperationException();
  }

}
