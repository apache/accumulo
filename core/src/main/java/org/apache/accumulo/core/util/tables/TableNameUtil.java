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
package org.apache.accumulo.core.util.tables;

import static org.apache.accumulo.core.util.Validators.EXISTING_TABLE_NAME;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.util.Pair;

public class TableNameUtil {
  // static utility only; don't allow instantiation
  private TableNameUtil() {}

  public static String qualified(String tableName) {
    return qualified(tableName, Namespace.DEFAULT.name());
  }

  public static String qualified(String tableName, String defaultNamespace) {
    Pair<String,String> qualifiedTableName = qualify(tableName, defaultNamespace);
    if (Namespace.DEFAULT.name().equals(qualifiedTableName.getFirst())) {
      return qualifiedTableName.getSecond();
    } else {
      return qualifiedTableName.toString("", ".", "");
    }
  }

  public static Pair<String,String> qualify(String tableName) {
    return qualify(tableName, Namespace.DEFAULT.name());
  }

  private static Pair<String,String> qualify(String tableName, String defaultNamespace) {
    EXISTING_TABLE_NAME.validate(tableName);
    if (tableName.contains(".")) {
      String[] s = tableName.split("\\.", 2);
      return new Pair<>(s[0], s[1]);
    }
    return new Pair<>(defaultNamespace, tableName);
  }
}
