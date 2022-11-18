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
package org.apache.accumulo.core.clientImpl;

import java.util.Collection;

import org.apache.accumulo.core.dataImpl.KeyExtent;

public enum TabletType {
  ROOT, METADATA, USER;

  public static TabletType type(KeyExtent ke) {
    if (ke.isRootTablet()) {
      return ROOT;
    }
    if (ke.isMeta()) {
      return METADATA;
    }
    return USER;
  }

  public static TabletType type(Collection<KeyExtent> extents) {
    if (extents.isEmpty()) {
      throw new IllegalArgumentException();
    }

    TabletType ttype = null;

    for (KeyExtent extent : extents) {
      if (ttype == null) {
        ttype = type(extent);
      } else if (ttype != type(extent)) {
        throw new IllegalArgumentException(
            "multiple extent types not allowed " + ttype + " " + type(extent));
      }
    }

    return ttype;
  }
}
