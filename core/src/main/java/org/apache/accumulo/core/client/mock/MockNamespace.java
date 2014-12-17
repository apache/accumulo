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
package org.apache.accumulo.core.client.mock;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.NamespacePermission;

public class MockNamespace {

  final Map<String,String> settings;
  Map<String,EnumSet<NamespacePermission>> userPermissions = new HashMap<String,EnumSet<NamespacePermission>>();

  public MockNamespace() {
    settings = new HashMap<String,String>();
    for (Entry<String,String> entry : AccumuloConfiguration.getDefaultConfiguration()) {
      String key = entry.getKey();
      if (key.startsWith(Property.TABLE_PREFIX.getKey())) {
        settings.put(key, entry.getValue());
      }
    }
  }

  public List<String> getTables(MockAccumulo acu) {
    List<String> l = new LinkedList<String>();
    for (String t : acu.tables.keySet()) {
      if (acu.tables.get(t).getNamespace().equals(this)) {
        l.add(t);
      }
    }
    return l;
  }
}
