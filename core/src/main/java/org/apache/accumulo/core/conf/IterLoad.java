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
package org.apache.accumulo.core.conf;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class IterLoad {

  Collection<IterInfo> iters;
  Map<String,Map<String,String>> iterOpts;
  IteratorEnvironment iteratorEnvironment;
  boolean useAccumuloClassLoader;
  String context;
  Map<String,Class<SortedKeyValueIterator<Key,Value>>> classCache;

  public IterLoad iters(Collection<IterInfo> iters) {
    this.iters = iters;
    return this;
  }

  public IterLoad iterOpts(Map<String,Map<String,String>> iterOpts) {
    this.iterOpts = iterOpts;
    return this;
  }

  public IterLoad iterEnv(IteratorEnvironment iteratorEnvironment) {
    this.iteratorEnvironment = iteratorEnvironment;
    return this;
  }

  public IterLoad useAccumuloClassLoader(boolean useAccumuloClassLoader) {
    this.useAccumuloClassLoader = useAccumuloClassLoader;
    return this;
  }

  public IterLoad context(String context) {
    this.context = context;
    return this;
  }

  public IterLoad classCache(Map<String,Class<SortedKeyValueIterator<Key,Value>>> classCache) {
    this.classCache = classCache;
    return this;
  }
}
