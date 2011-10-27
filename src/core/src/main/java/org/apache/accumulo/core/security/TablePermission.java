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
import java.util.List;

public enum TablePermission {
  // One can add new permissions, with new numbers, but please don't change or use numbers previously assigned
  // CREATE_LOCALITY_GROUP(0),
  // DROP_LOCALITY_GROUP(1),
  READ((byte) 2),
  WRITE((byte) 3),
  BULK_IMPORT((byte) 4),
  ALTER_TABLE((byte) 5),
  GRANT((byte) 6),
  DROP_TABLE((byte) 7);
  
  final private byte permID;
  
  final private static TablePermission mapping[] = new TablePermission[8];
  static {
    for (TablePermission perm : TablePermission.values())
      mapping[perm.permID] = perm;
  }
  
  private TablePermission(byte id) {
    this.permID = id;
  }
  
  public byte getId() {
    return this.permID;
  }
  
  public static List<String> printableValues() {
    TablePermission[] a = TablePermission.values();
    
    List<String> list = new ArrayList<String>(a.length);
    
    for (TablePermission p : a)
      list.add("Table." + p);
    
    return list;
  }
  
  public static TablePermission getPermissionById(byte id) {
    TablePermission result = mapping[id];
    if (result != null)
      return result;
    throw new IndexOutOfBoundsException("No such permission");
  }
  
}
