/**
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
package org.apache.accumulo.core.iterators;

/**
 * An interface designed to be added to containers to specify what
 * can be left out when iterating over the contents of that container.
 */
public interface Filterer<K,V> {
  /**
   * Either optionally or always leave out entries for which the given Predicate evaluates to false 
   * @param filter The predicate that specifies whether an entry can be left out
   * @param required If true, entries that don't pass the filter must be left out. If false, then treat
   *          purely as a potential optimization.
   */
  public void applyFilter(Predicate<K,V> filter, boolean required);
}
