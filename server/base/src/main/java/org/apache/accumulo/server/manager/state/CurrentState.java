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
package org.apache.accumulo.server.manager.state;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.TServerInstance;

public interface CurrentState {

  Set<TableId> onlineTables();

  Set<TServerInstance> onlineTabletServers();

  Map<String,Set<TServerInstance>> tServerResourceGroups();

  Set<TServerInstance> shutdownServers();

  Collection<MergeInfo> merges();

  /**
   * Provide an immutable snapshot view of migrating tablets. Objects contained in the set may still
   * be mutable.
   */
  Set<KeyExtent> migrationsSnapshot();

  ManagerState getManagerState();

  // ELASTICITIY_TODO it would be nice if this method could take DataLevel as an argument and only
  // retrieve information about compactions in that data level. Attempted this and a lot of
  // refactoring was needed to get that small bit of information to this method. Would be best to
  // address this after issue. May be best to attempt this after #3576.
  Map<Long,Map<String,String>> getCompactionHints();
}
