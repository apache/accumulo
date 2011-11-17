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
package org.apache.accumulo.core.iterators.filter;

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.OptionDescriber;

/**
 * @deprecated since 1.4
 * @use org.apache.accumulo.core.iterators.user.AgeOffFilter
 **/
public class AgeOffFilter implements Filter, OptionDescriber {
  private long threshold;
  private long currentTime;
  
  @Override
  public boolean accept(Key k, Value v) {
    if (currentTime - k.getTimestamp() > threshold)
      return false;
    return true;
  }
  
  @Override
  public void init(Map<String,String> options) {
    threshold = -1;
    if (options == null)
      throw new IllegalArgumentException("ttl must be set for AgeOffFilter");
    
    String ttl = options.get("ttl");
    if (ttl == null)
      throw new IllegalArgumentException("ttl must be set for AgeOffFilter");
    
    threshold = Long.parseLong(ttl);
    
    String time = options.get("currentTime");
    if (time != null)
      currentTime = Long.parseLong(time);
    else
      currentTime = System.currentTimeMillis();
    
    // add sanity checks for threshold and currentTime?
  }
  
  @Override
  public IteratorOptions describeOptions() {
    Map<String,String> options = new TreeMap<String,String>();
    options.put("ttl", "time to live (milliseconds)");
    options.put("currentTime", "if set, use the given value as the absolute time in milliseconds as the current time of day");
    return new IteratorOptions("ageoff", "AgeOffFilter removes entries with timestamps more than <ttl> milliseconds old", options, null);
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    Long.parseLong(options.get("ttl"));
    return true;
  }
}
