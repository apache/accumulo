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
package org.apache.accumulo.tserver;

import java.util.Set;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.tserver.compactions.Compactable;

public class OpeningAndOnlineCompactables {
  public final Set<KeyExtent> opening;
  public final Iterable<Compactable> open;

  public OpeningAndOnlineCompactables(Set<KeyExtent> openingSnapshot,
      Iterable<Compactable> openSnapshot) {
    this.opening = openingSnapshot;
    this.open = openSnapshot;
  }
}
