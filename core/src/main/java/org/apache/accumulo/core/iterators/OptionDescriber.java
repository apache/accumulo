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
package org.apache.accumulo.core.iterators;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The OptionDescriber interface allows you to set up iterator properties interactively in the
 * accumulo shell. If your iterator and/or filter must implement this interface for the interactive
 * part. The alternative would be to manually set configuration options with the config -t tableName
 * property=value. If you go the manual route, be careful to use the correct structure for the
 * property and to set all the properties required for the iterator.
 *
 * OptionDescribers will need to implement two methods: {@code describeOptions()} which returns an
 * instance of {@link IteratorOptions} and {@code validateOptions(Map<String,String> options)} which
 * is intended to throw an exception or return false if the options are not acceptable.
 */
public interface OptionDescriber {
  class IteratorOptions {
    public LinkedHashMap<String,String> namedOptions;
    public ArrayList<String> unnamedOptionDescriptions;
    public String name;
    public String description;

    /**
     * IteratorOptions holds the name, description, and option information for an iterator.
     *
     * @param name is the distinguishing name for the iterator or filter
     * @param description is a description of the iterator or filter
     * @param namedOptions is a map from specifically named options to their descriptions (null if
     *        unused) e.g., the AgeOffFilter requires a parameter called "ttl", so its namedOptions
     *        = Collections.singletonMap("ttl", "time to live (milliseconds)")
     * @param unnamedOptionDescriptions is a list of descriptions of additional options that don't
     *        have fixed names (null if unused). The descriptions are intended to describe a
     *        category, and the user will provide parameter names and values in that category; e.g.,
     *        the FilteringIterator needs a list of Filters intended to be named by their priority
     *        numbers, so it's<br>
     *        {@code unnamedOptionDescriptions = Collections}<br>
     *        {@code .singletonList("<filterPriorityNumber> <ageoff|regex|filterClass>")}
     */
    public IteratorOptions(String name, String description, Map<String,String> namedOptions,
        List<String> unnamedOptionDescriptions) {
      this.name = name;
      this.namedOptions = null;
      if (namedOptions != null) {
        this.namedOptions = new LinkedHashMap<>(namedOptions);
      }
      this.unnamedOptionDescriptions = null;
      if (unnamedOptionDescriptions != null) {
        this.unnamedOptionDescriptions = new ArrayList<>(unnamedOptionDescriptions);
      }
      this.description = description;
    }

    public Map<String,String> getNamedOptions() {
      return namedOptions;
    }

    public List<String> getUnnamedOptionDescriptions() {
      return unnamedOptionDescriptions;
    }

    public String getName() {
      return name;
    }

    public String getDescription() {
      return description;
    }

    public void setNamedOptions(Map<String,String> namedOptions) {
      this.namedOptions = new LinkedHashMap<>(namedOptions);
    }

    public void setUnnamedOptionDescriptions(List<String> unnamedOptionDescriptions) {
      this.unnamedOptionDescriptions = new ArrayList<>(unnamedOptionDescriptions);
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public void addNamedOption(String name, String description) {
      if (namedOptions == null) {
        namedOptions = new LinkedHashMap<>();
      }
      namedOptions.put(name, description);
    }

    public void addUnnamedOption(String description) {
      if (unnamedOptionDescriptions == null) {
        unnamedOptionDescriptions = new ArrayList<>();
      }
      unnamedOptionDescriptions.add(description);
    }
  }

  /**
   * Gets an iterator options object that contains information needed to configure this iterator.
   * This object will be used by the accumulo shell to prompt the user to input the appropriate
   * information.
   *
   * @return an iterator options object
   */
  IteratorOptions describeOptions();

  /**
   * Check to see if an options map contains all options required by an iterator and that the option
   * values are in the expected formats.
   *
   * @param options a map of option names to option values
   * @return true if options are valid, false otherwise (IllegalArgumentException preferred)
   * @exception IllegalArgumentException if there are problems with the options
   */
  boolean validateOptions(Map<String,String> options);
}
