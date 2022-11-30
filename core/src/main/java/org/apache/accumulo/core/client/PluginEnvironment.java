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
package org.apache.accumulo.core.client;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.TableId;

/**
 * This interface exposes Accumulo system level information to plugins in a stable manner. The
 * purpose of this interface is to insulate plugins from internal refactorings and changes to
 * Accumulo.
 *
 * @since 2.1.0
 */
public interface PluginEnvironment {

  /**
   * @since 2.1.0
   */
  interface Configuration extends Iterable<Entry<String,String>> {

    /**
     * Properties with a default value will always return something when calling
     * {@link #get(String)}, even if a user never set the property. The method allows checking if a
     * user set a property.
     *
     * @return true if a user set this property and false if a user did not set it.
     * @since 2.1.0
     */
    boolean isSet(String key);

    /**
     * @return The value for a single property or null if not present. Sensitive properties are
     *         intentionally not returned in order to prevent inadvertent logging of them. If your
     *         plugin needs sensitive properties a getSensitive method could be added.
     */
    String get(String key);

    /**
     * Returns all properties with a given prefix
     *
     * @param prefix prefix of properties to be returned. Include the trailing '.' in the prefix.
     * @return all properties with a given prefix
     * @since 2.1.0
     */
    Map<String,String> getWithPrefix(String prefix);

    /**
     * Users can set arbitrary custom properties in Accumulo using the prefix
     * {@code general.custom.}. This method will return all properties with that prefix, stripping
     * the prefix. For example, assume the following properties were set :
     *
     * <pre>
     * {@code
     *   general.custom.prop1=123
     *   general.custom.prop2=abc
     * }
     * </pre>
     *
     * Then this function would return a map containing {@code [prop1=123,prop2=abc]}.
     *
     */
    Map<String,String> getCustom();

    /**
     * This method appends the prefix {@code general.custom} and gets the property.
     *
     * @return The same as calling {@code getCustom().get(keySuffix)} OR
     *         {@code get("general.custom."+keySuffix)}
     */
    String getCustom(String keySuffix);

    /**
     * Users can set arbitrary custom table properties in Accumulo using the prefix
     * {@code table.custom.}. This method will return all properties with that prefix, stripping the
     * prefix. For example, assume the following properties were set :
     *
     * <pre>
     * {@code
     *   table.custom.tp1=ch1
     *   table.custom.tp2=bh2
     * }
     * </pre>
     *
     * Then this function would return a map containing {@code [tp1=ch1,tp2=bh2]}.
     *
     */
    Map<String,String> getTableCustom();

    /**
     * This method appends the prefix {@code table.custom} and gets the property.
     *
     * @return The same as calling {@code getTableCustom().get(keySuffix)} OR
     *         {@code get("table.custom."+keySuffix)}
     */
    String getTableCustom(String keySuffix);

    /**
     * Returns an iterator over all properties. This may be inefficient, consider opening an issue
     * if you have a use case that is only satisfied by this. Sensitive properties are intentionally
     * suppressed in order to prevent inadvertent logging of them.
     */
    @Override
    Iterator<Entry<String,String>> iterator();

    /**
     * Returns a derived value from this Configuration. The returned value supplier is thread-safe
     * and attempts to avoid re-computation of the response. The intended use for a derived value is
     * to ensure that configuration changes that may be made in Zookeeper, for example, are always
     * reflected in the returned value.
     */
    <T> Supplier<T> getDerived(Function<Configuration,T> computeDerivedValue);
  }

  /**
   * @return A view of Accumulo's system level configuration. This is backed by system level config
   *         in zookeeper, which falls back to site configuration, which falls back to the default
   *         configuration.
   */
  Configuration getConfiguration();

  /**
   * @return a view of a table's configuration. When requesting properties that start with
   *         {@code table.} the returned configuration may give different values for different
   *         tables. For other properties the returned configuration will return the same value as
   *         {@link #getConfiguration()}.
   *
   */
  Configuration getConfiguration(TableId tableId);

  /**
   * Many Accumulo plugins are given table IDs as this is what Accumulo uses internally to identify
   * tables. If a plugin needs to log debugging information it can call this method to get the table
   * name.
   */
  String getTableName(TableId tableId) throws TableNotFoundException;

  /**
   * Instantiate a class using Accumulo's system classloader. The class must have a no argument
   * constructor.
   *
   * @param className Fully qualified name of the class.
   * @param base The expected super type of the class.
   */
  <T> T instantiate(String className, Class<T> base) throws Exception;

  /**
   * Instantiate a class using Accumulo's per table classloader. The class must have a no argument
   * constructor.
   *
   * @param className Fully qualified name of the class.
   * @param base The expected super type of the class.
   */
  <T> T instantiate(TableId tableId, String className, Class<T> base) throws Exception;
}
