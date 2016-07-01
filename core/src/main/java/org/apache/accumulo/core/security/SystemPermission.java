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
package org.apache.accumulo.core.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Accumulo system permissions. Each permission has an associated byte ID.
 */
public enum SystemPermission {
  /*
   * One may add new permissions, but new permissions must use new numbers. Current numbers in use must not be changed.
   */
  GRANT((byte) 0),
  CREATE_TABLE((byte) 1),
  DROP_TABLE((byte) 2),
  ALTER_TABLE((byte) 3),
  CREATE_USER((byte) 4),
  DROP_USER((byte) 5),
  ALTER_USER((byte) 6),
  SYSTEM((byte) 7),
  CREATE_NAMESPACE((byte) 8),
  DROP_NAMESPACE((byte) 9),
  ALTER_NAMESPACE((byte) 10),
  OBTAIN_DELEGATION_TOKEN((byte) 11);

  private byte permID;

  private static HashMap<Byte,SystemPermission> mapping;
  static {
    mapping = new HashMap<>(SystemPermission.values().length);
    for (SystemPermission perm : SystemPermission.values())
      mapping.put(perm.permID, perm);
  }

  private SystemPermission(byte id) {
    this.permID = id;
  }

  /**
   * Gets the byte ID of this permission.
   *
   * @return byte ID
   */
  public byte getId() {
    return this.permID;
  }

  /**
   * Returns a list of printable permission values.
   *
   * @return list of system permission values, as "System." + permission name
   */
  public static List<String> printableValues() {
    SystemPermission[] a = SystemPermission.values();

    List<String> list = new ArrayList<>(a.length);

    for (SystemPermission p : a)
      list.add("System." + p);

    return list;
  }

  /**
   * Gets the permission matching the given byte ID.
   *
   * @param id
   *          byte ID
   * @return system permission
   * @throws IndexOutOfBoundsException
   *           if the byte ID is invalid
   */
  public static SystemPermission getPermissionById(byte id) {
    if (mapping.containsKey(id))
      return mapping.get(id);
    throw new IndexOutOfBoundsException("No such permission");
  }
}
