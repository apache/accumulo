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
package org.apache.accumulo.core.client.impl;

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

import org.apache.accumulo.core.client.Instance;

public class Namespace {
  public static final String ACCUMULO = "accumulo";
  public static final String DEFAULT = "";
  public static final String SEPARATOR = ".";

  /**
   * Object representing an internal Namespace ID. This class was created to help with type safety. For help obtaining the value of a namespace ID from
   * Zookeeper, see {@link Namespaces#getNamespaceId(Instance, String)}
   *
   * Uses an internal WeakHashMap and private constructor for storing a WeakReference of every Namespace.ID. Therefore, a Namespace.ID can't be instantiated
   * outside this class and is accessed by calling Namespace.ID.{@link #of(String)}.
   */
  public static class ID extends AbstractId {
    private static final long serialVersionUID = 8931104141709170293L;
    static final WeakHashMap<String,WeakReference<Namespace.ID>> namespaceIds = new WeakHashMap<>();

    public static final ID ACCUMULO = of("+accumulo");
    public static final ID DEFAULT = of("+default");

    private ID(String canonical) {
      super(canonical);
    }

    /**
     * Get a Namespace.ID object for the provided canonical string.
     *
     * @param canonical
     *          Namespace ID string
     * @return Namespace.ID object
     */
    public static Namespace.ID of(final String canonical) {
      return dedupeNamespaceId(canonical);
    }

    private static Namespace.ID dedupeNamespaceId(String idString) {
      synchronized (namespaceIds) {
        Namespace.ID namespaceId;
        WeakReference<Namespace.ID> namespaceIdRef = namespaceIds.get(idString);
        if (namespaceIdRef != null) {
          namespaceId = namespaceIdRef.get();
          if (namespaceId != null) {
            return namespaceId;
          }
        }

        namespaceId = new ID(idString);
        namespaceIds.put(idString, new WeakReference<>(namespaceId));
        return namespaceId;
      }
    }

  }
}
