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

import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

/**
 * Builder class for setting up the iterator stack.
 */
public class IteratorBuilder {
  Collection<IterInfo> iters;
  Map<String,Map<String,String>> iterOpts;
  IteratorEnvironment iteratorEnvironment;
  boolean useAccumuloClassLoader;
  String context = null;
  boolean useClassCache = false;

  IteratorBuilder() {}

  /**
   * Start building the iterator builder.
   */
  public static IteratorBuilderImpl builder(Collection<IterInfo> iters) {
    return new IteratorBuilderImpl(iters);
  }

  public interface IteratorBuilderEnv {
    /**
     * Set the iteratorEnvironment.
     */
    IteratorBuilderCassLoader env(IteratorEnvironment iteratorEnvironment);
  }

  public interface IteratorBuilderCassLoader {
    /**
     * Pass true to useAccumuloClassLoader or false otherwise. If true, a context is required.
     */
    IteratorBuilderOptions useClassLoader(boolean useAccumuloClassLoader);
  }

  public interface IteratorBuilderOptions extends IteratorBuilderCassLoader {
    /**
     * Option to iterator classes when loading, defaults to false.
     */
    IteratorBuilderOptions useClassCache(boolean useClassCache);

    /**
     * Option to set the String context, defaults to null. This is required to use the Accumulo
     * class loader.
     */
    IteratorBuilderOptions context(String context);

    /**
     * Finish building and return the completed IteratorBuilder.
     */
    IteratorBuilder build();
  }

}
