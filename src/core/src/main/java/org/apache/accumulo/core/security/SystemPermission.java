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

public enum SystemPermission {
  // One can add new permissions, with new numbers, but please don't change or use numbers previously assigned
  GRANT((byte) 0),
  CREATE_TABLE((byte) 1),
  DROP_TABLE((byte) 2),
  ALTER_TABLE((byte) 3),
  CREATE_USER((byte) 4),
  DROP_USER((byte) 5),
  ALTER_USER((byte) 6),
  SYSTEM((byte) 7);
  
  private byte permID;
  
  private static HashMap<Byte,SystemPermission> mapping;
  static {
    mapping = new HashMap<Byte,SystemPermission>(SystemPermission.values().length);
    for (SystemPermission perm : SystemPermission.values())
      mapping.put(perm.permID, perm);
  }
  
  private SystemPermission(byte id) {
    this.permID = id;
  }
  
  public byte getId() {
    return this.permID;
  }
  
  public static List<String> printableValues() {
    SystemPermission[] a = SystemPermission.values();
    
    List<String> list = new ArrayList<String>(a.length);
    
    for (SystemPermission p : a)
      list.add("System." + p);
    
    return list;
  }
  
  public static SystemPermission getPermissionById(byte id) {
    if (mapping.containsKey(id))
      return mapping.get(id);
    throw new IndexOutOfBoundsException("No such permission");
  }
}
