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
package org.apache.accumulo.server.security.handler;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

/**
 * Pluggable permissions module returned by {@link SecurityModule#perm()} ()}.
 * Lots of room for improvement here.
 *
 * @since 2.1
 */
public interface Perm {

  boolean hasSystem(String user, SystemPermission perm);
  boolean hasCachedSystemPermission(String user, SystemPermission permission);

  boolean hasTable(String user, TableId tableId, TablePermission perm)
      throws TableNotFoundException;
  boolean hasCachedTablePermission(String user, TableId tableId,
                                   TablePermission permission);

  boolean hasNamespace(String user, NamespaceId namespaceId, NamespacePermission perm)
      throws NamespaceNotFoundException;
  boolean hasCachedNamespacePermission(String user, NamespaceId namespaceId,
                                       NamespacePermission permission);

  void grantSystem(String user, SystemPermission perm) throws AccumuloSecurityException;

  void revokeSystem(String user, SystemPermission perm) throws AccumuloSecurityException;

  void grantTable(String user, TableId tableId, TablePermission perm)
      throws AccumuloSecurityException, TableNotFoundException;

  void revokeTable(String user, TableId tableId, TablePermission perm)
      throws AccumuloSecurityException, TableNotFoundException;

  void grantNamespace(String user, NamespaceId namespaceId, NamespacePermission perm)
      throws AccumuloSecurityException, NamespaceNotFoundException;

  void revokeNamespace(String user, NamespaceId namespaceId, NamespacePermission perm)
      throws AccumuloSecurityException, NamespaceNotFoundException;

  void cleanTableOrNamespace(AbstractId<?> tableOrNs, String zkTableOrNsPerms)
          throws AccumuloSecurityException;

}
