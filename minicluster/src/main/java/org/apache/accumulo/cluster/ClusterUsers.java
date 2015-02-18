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
package org.apache.accumulo.cluster;

import org.apache.accumulo.core.security.SystemPermission;

/**
 * General interface to get user details for an {@link AccumuloCluster}.
 */
public interface ClusterUsers {

  /**
   * The "root" user. A user which has administrative permissions {@link SystemPermission#SYSTEM} and the like.
   */
  ClusterUser getAdminUser();

  /**
   * Get an unprivileged user. Multiple are created
   *
   * @param offset
   *          the user to return
   * @return the user denoted by the given offset
   */
  ClusterUser getUser(int offset);
}
