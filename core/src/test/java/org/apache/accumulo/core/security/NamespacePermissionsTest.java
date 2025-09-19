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
package org.apache.accumulo.core.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

public class NamespacePermissionsTest {
  @Test
  public void testEnsureEquivalencies() {
    EnumSet<NamespacePermission> set = EnumSet.allOf(NamespacePermission.class);

    for (TablePermission permission : TablePermission.values()) {
      var equivalent = NamespacePermission.getEquivalent(permission);
      if (equivalent != null) {
        assertTrue(set.remove(equivalent));
        assertEquals(permission.name(), equivalent.name());
      }
    }

    // these namespace permissions have no equivalent table permission
    assertEquals(EnumSet.of(NamespacePermission.ALTER_NAMESPACE, NamespacePermission.DROP_NAMESPACE,
        NamespacePermission.CREATE_TABLE), set);

    set = EnumSet.allOf(NamespacePermission.class);
    for (SystemPermission permission : SystemPermission.values()) {
      if (permission == SystemPermission.GRANT) {
        assertThrows(IllegalArgumentException.class,
            () -> NamespacePermission.getEquivalent(permission), "GRANT has no equivalent");
      } else {
        var equivalent = NamespacePermission.getEquivalent(permission);
        if (equivalent != null) {
          assertTrue(set.remove(equivalent));
          assertEquals(permission.name(), equivalent.name());
        }

      }
    }

    // these namespace permissions have no equivalent system permission
    assertEquals(EnumSet.of(NamespacePermission.READ, NamespacePermission.WRITE,
        NamespacePermission.GRANT, NamespacePermission.BULK_IMPORT), set);
  }
}
