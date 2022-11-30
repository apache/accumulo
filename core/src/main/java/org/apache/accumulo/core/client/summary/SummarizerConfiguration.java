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
package org.apache.accumulo.core.client.summary;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.accumulo.core.summary.SummarizerConfigurationUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * This class encapsulates the configuration needed to instantiate a {@link Summarizer}. It also
 * provides methods and documentation for setting the table properties that configure a Summarizer.
 *
 * @since 2.0.0
 */
public class SummarizerConfiguration {

  private final String className;
  private final Map<String,String> options;
  private int hashCode = 0;
  private final String configId;

  private SummarizerConfiguration(String className, String configId, Map<String,String> options) {
    this.className = className;
    this.options = Map.copyOf(options);

    if (configId == null) {
      ArrayList<String> keys = new ArrayList<>(this.options.keySet());
      Collections.sort(keys);
      Hasher hasher = Hashing.murmur3_32_fixed().newHasher();
      hasher.putString(className, UTF_8);
      for (String key : keys) {
        hasher.putString(key, UTF_8);
        hasher.putString(options.get(key), UTF_8);
      }

      this.configId = hasher.hash().toString();
    } else {
      this.configId = configId;
    }
  }

  /**
   * @return the name of a class that implements @link {@link Summarizer}.
   */
  public String getClassName() {
    return className;
  }

  /**
   * @return custom options for a {link @Summarizer}
   */
  public Map<String,String> getOptions() {
    return options;
  }

  /**
   * The propertyId is used when creating table properties for a summarizer. It's not used for
   * equality or hashCode for this class.
   */
  public String getPropertyId() {
    return configId;
  }

  @Override
  public String toString() {
    return className + " " + configId + " " + options;
  }

  /**
   * Compares the classname and options to determine equality.
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof SummarizerConfiguration) {
      SummarizerConfiguration osc = (SummarizerConfiguration) o;
      return className.equals(osc.className) && options.equals(osc.options);
    }

    return false;
  }

  /**
   * Hashes the classname and options to create a hashcode.
   */
  @Override
  public int hashCode() {
    if (hashCode == 0) {
      hashCode = 31 * options.hashCode() + className.hashCode();
    }
    return hashCode;
  }

  /**
   * Converts this configuration to Accumulo per table properties. The returned map has the
   * following key values. The {@code <configId>} below is from {@link #getPropertyId()}. The
   * {@code <optionKey>} and {@code <optionValue>} below are derived from the key values of
   * {@link #getOptions()}.
   *
   * <pre>
   * {@code
   *   table.summarizer.<configId>=<classname>
   *   table.summarizer.<configId>.opt.<optionKey1>=<optionValue1>
   *   table.summarizer.<configId>.opt.<optionKey2>=<optionValue2>
   *      .
   *      .
   *      .
   *   table.summarizer.<configId>.opt.<optionKeyN>=<optionValueN>
   * }
   * </pre>
   */
  public Map<String,String> toTableProperties() {
    return SummarizerConfigurationUtil.toTablePropertiesMap(Collections.singletonList(this));
  }

  /**
   * Encodes each configuration in the same way as {@link #toTableProperties()}.
   *
   * @throws IllegalArgumentException when there are duplicate values for {@link #getPropertyId()}
   */
  public static Map<String,String> toTableProperties(SummarizerConfiguration... configurations) {
    return SummarizerConfigurationUtil.toTablePropertiesMap(Arrays.asList(configurations));
  }

  /**
   * Encodes each configuration in the same way as {@link #toTableProperties()}.
   *
   * @throws IllegalArgumentException when there are duplicate values for {@link #getPropertyId()}
   */
  public static Map<String,String>
      toTableProperties(Collection<SummarizerConfiguration> configurations) {
    return SummarizerConfigurationUtil.toTablePropertiesMap(new ArrayList<>(configurations));
  }

  /**
   * Decodes table properties with the prefix {@code table.summarizer} into
   * {@link SummarizerConfiguration} objects. Table properties with prefixes other than
   * {@code table.summarizer} are ignored.
   */
  public static Collection<SummarizerConfiguration> fromTableProperties(Map<String,String> props) {
    return fromTableProperties(props.entrySet());
  }

  /**
   * @see #fromTableProperties(Map)
   */
  public static Collection<SummarizerConfiguration>
      fromTableProperties(Iterable<Entry<String,String>> props) {
    return SummarizerConfigurationUtil.getSummarizerConfigs(props);
  }

  /**
   * @since 2.0.0
   */
  public static class Builder {
    private final String className;
    private final ImmutableMap.Builder<String,String> imBuilder = ImmutableMap.builder();
    private String configId = null;
    private static final Predicate<String> ALPHANUM = Pattern.compile("\\w+").asMatchPredicate();

    private Builder(String className) {
      this.className = className;
    }

    /**
     * Sets the id used when generating table properties. Setting this is optional. If not set, an
     * id is generated using hashing that will likely be unique.
     *
     * @param propId This id is used when converting a {@link SummarizerConfiguration} to table
     *        properties. Since tables can have multiple summarizers, make sure its unique.
     *
     * @see SummarizerConfiguration#toTableProperties()
     */
    public Builder setPropertyId(String propId) {
      Preconditions.checkArgument(ALPHANUM.test(propId), "Config Id %s is not alphanum", propId);
      this.configId = propId;
      return this;
    }

    /**
     * Adds an option that Summarizers can use when constructing Collectors and Combiners.
     *
     * @return this
     *
     * @see SummarizerConfiguration#getOptions()
     */
    public Builder addOption(String key, String value) {
      Preconditions.checkArgument(ALPHANUM.test(key), "Option Id %s is not alphanum", key);
      imBuilder.put(key, value);
      return this;
    }

    /**
     * Adds an option that Summarizers can use when constructing Collectors and Combiners.
     *
     * @return this
     *
     * @see SummarizerConfiguration#getOptions()
     */
    public Builder addOption(String key, long value) {
      return addOption(key, Long.toString(value));
    }

    /**
     * Convenience method for adding multiple options. The following
     *
     * <pre>
     * {@code
     * builder.addOptions("opt1", "val1", "opt2", "val2", "opt3", "val3")
     * }
     * </pre>
     *
     * <p>
     * is equivalent to
     *
     * <pre>
     * {@code
     * builder.addOption("opt1", "val1");
     * builder.addOption("opt2", "val2");
     * builder.addOption("opt3", "val3");
     * }
     * </pre>
     *
     * @param keyValuePairs This array must have an even and positive number of elements.
     * @return this
     * @see SummarizerConfiguration#getOptions()
     */
    public Builder addOptions(String... keyValuePairs) {
      Preconditions.checkArgument(keyValuePairs.length % 2 == 0 && keyValuePairs.length > 0,
          "Require an even, positive number of arguments, got %s", keyValuePairs.length);
      for (int i = 1; i < keyValuePairs.length; i += 2) {
        addOption(keyValuePairs[i - 1], keyValuePairs[i]);
      }
      return this;
    }

    /**
     * @param options Each entry in the map is passed to {@link #addOption(String, String)}
     * @return this
     *
     * @see SummarizerConfiguration#getOptions()
     */
    public Builder addOptions(Map<String,String> options) {
      options.forEach(this::addOption);
      return this;
    }

    public SummarizerConfiguration build() {
      return new SummarizerConfiguration(className, configId, imBuilder.build());
    }
  }

  /**
   * Call this method to initiate a chain of fluent method calls to a create an immutable
   * {@link SummarizerConfiguration}
   *
   * @param className The fully qualified name of a class that implements {@link Summarizer}.
   */
  public static Builder builder(String className) {
    return new Builder(className);
  }

  /**
   * @see #builder(String)
   */
  public static Builder builder(Class<? extends Summarizer> clazz) {
    return new Builder(clazz.getName());
  }
}
