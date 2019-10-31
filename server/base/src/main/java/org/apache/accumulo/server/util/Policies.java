/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.accumulo.server.util;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.conf.NamespaceConfiguration;
import org.apache.accumulo.server.conf.TableConfiguration;

public class Policies {
  private String storagePolicy;
  private String encodingPolicy;

  private Policies(String storage, String encoding) {
    storagePolicy = storage;
    encodingPolicy = encoding;
  }

  public String getEncodingPolicy() {
    return encodingPolicy;
  }

  public String getStoragePolicy() {
    return storagePolicy;
  }

  public static Policies getPoliciesForTable(TableConfiguration tableConf) {
    var storagePolicy = tableConf.get(Property.TABLE_STORAGE_POLICY);
    var encoding = tableConf.get(Property.TABLE_CODING_POLICY);
    return new Policies(storagePolicy, encoding);
  }

  public static Policies getPoliciesForNamespace(NamespaceConfiguration namespaceConf) {
    var storagePolicy = namespaceConf.get(Property.TABLE_STORAGE_POLICY);
    var encoding = namespaceConf.get(Property.TABLE_CODING_POLICY);
    return new Policies(storagePolicy, encoding);
  }
}
