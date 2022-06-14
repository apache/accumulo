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
package org.apache.accumulo.core.iteratorsImpl;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

public class IteratorBuilderImpl
    implements IteratorBuilder.IteratorBuilderEnv, IteratorBuilder.IteratorBuilderOptions {

  Collection<IterInfo> iters;
  Map<String,Map<String,String>> iterOpts;
  IteratorEnvironment iteratorEnvironment;
  boolean useAccumuloClassLoader = false;
  String context = null;
  boolean useClassCache = false;

  public IteratorBuilderImpl(Collection<IterInfo> iters) {
    this.iters = iters;
  }

  public IteratorBuilder.IteratorBuilderEnv opts(Map<String,Map<String,String>> iterOpts) {
    this.iterOpts = iterOpts;
    return this;
  }

  @Override
  public IteratorBuilder.IteratorBuilderOptions env(IteratorEnvironment iteratorEnvironment) {
    this.iteratorEnvironment = iteratorEnvironment;
    return this;
  }

  @Override
  public IteratorBuilder.IteratorBuilderOptions useClassLoader(String context) {
    this.useAccumuloClassLoader = true;
    this.context = context;
    return this;
  }

  @Override
  public IteratorBuilder.IteratorBuilderOptions useClassCache(boolean useClassCache) {
    this.useClassCache = useClassCache;
    return this;
  }

  @Override
  public IteratorBuilder build() {
    var ib = new IteratorBuilder();
    ib.iters = this.iters;
    ib.iterOpts = this.iterOpts;
    ib.iteratorEnvironment = this.iteratorEnvironment;
    ib.useAccumuloClassLoader = this.useAccumuloClassLoader;
    ib.context = this.context;
    ib.useClassCache = this.useClassCache;
    return ib;
  }
}
