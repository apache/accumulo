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
package org.apache.accumulo.core.iterators;

import java.util.List;
import java.util.Map;

/*
 * The OptionDescriber interface allows you to set up iterator properties interactively in the accumulo shell.
 * If your iterator and/or filter must implement this interface for the interactive part.  The alternative would
 * be to manually set configuration options with the config -t tableName property=value.  If you go the manual
 * route, be careful to use the correct structure for the property and to set all the properties required for 
 * the iterator.
 * 
 * OptionDescribers will need to implement two methods:
 *      describeOptions() which returns an instance of IteratorOptions
 * 	and validateOptions(Map<String,String> options) which is intended to throw an exception or return false if the options are not acceptable.
 * 
 */
public interface OptionDescriber {
  public static class IteratorOptions {
    public final Map<String,String> namedOptions;
    public final List<String> unnamedOptionDescriptions;
    public final String name;
    public final String description;
    
    /*
     * IteratorOptions requires the following: name is the distinguishing name for the iterator or filter description is a description of the iterator or filter
     * namedOptions is a map from specifically named options to their descriptions (null if unused) e.g., the AgeOffFilter requires a parameter called "ttl", so
     * its namedOptions = Collections.singletonMap("ttl", "time to live (milliseconds)") unnamedOptionDescriptions is a list of descriptions of additional
     * options that don't have fixed names (null if unused) the descriptions are intended to describe a category, and the user will provide parameter names and
     * values in that category e.g., the FilteringIterator needs a list of Filters intended to be named by their priority numbers, so its
     * unnamedOptionDescriptions = Collections.singletonList("<filterPriorityNumber> <ageoff|regex|filterClass>")
     */
    public IteratorOptions(String name, String description, Map<String,String> namedOptions, List<String> unnamedOptionDescriptions) {
      this.name = name;
      this.namedOptions = namedOptions;
      this.unnamedOptionDescriptions = unnamedOptionDescriptions;
      this.description = description;
    }
    
  }
  
  public IteratorOptions describeOptions();
  
  public boolean validateOptions(Map<String,String> options);
}
