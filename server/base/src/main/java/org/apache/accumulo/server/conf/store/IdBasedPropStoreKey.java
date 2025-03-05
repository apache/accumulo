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
package org.apache.accumulo.server.conf.store;

import org.apache.accumulo.core.data.AbstractId;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Provides a strongly-typed id for storing properties in ZooKeeper based on a specific AbstractId
 * type. The path is based on the AbstractId type, and is the canonical String version of this key.
 */
public abstract class IdBasedPropStoreKey<ID_TYPE extends AbstractId<ID_TYPE>>
    extends PropStoreKey {

  private final ID_TYPE id;

  protected IdBasedPropStoreKey(final String path, final ID_TYPE id) {
    super(path);
    this.id = id;
  }

  public @NonNull ID_TYPE getId() {
    return id;
  }

}
