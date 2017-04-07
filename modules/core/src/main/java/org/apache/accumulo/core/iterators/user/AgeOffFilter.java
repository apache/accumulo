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
package org.apache.accumulo.core.iterators.user;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * A filter that ages off key/value pairs based on the Key's timestamp. It removes an entry if its timestamp is less than currentTime - threshold.
 *
 * This filter requires a "ttl" option, in milliseconds, to determine the age off threshold.
 */
public class AgeOffFilter extends Filter {
  private static final String TTL = "ttl";
  private static final String CURRENT_TIME = "currentTime";
  private long threshold;
  private long currentTime;

  /**
   * Accepts entries whose timestamps are less than currentTime - threshold.
   *
   * @see org.apache.accumulo.core.iterators.Filter#accept(org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value)
   */
  @Override
  public boolean accept(Key k, Value v) {
    if (currentTime - k.getTimestamp() > threshold)
      return false;
    return true;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    if (options == null)
      throw new IllegalArgumentException(TTL + " must be set for AgeOffFilter");

    String ttl = options.get(TTL);
    if (ttl == null)
      throw new IllegalArgumentException(TTL + " must be set for AgeOffFilter");

    super.init(source, options, env);
    threshold = Long.parseLong(ttl);

    String time = options.get(CURRENT_TIME);
    if (time != null)
      currentTime = Long.parseLong(time);
    else
      currentTime = System.currentTimeMillis();

    // add sanity checks for threshold and currentTime?
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    AgeOffFilter copy = (AgeOffFilter) super.deepCopy(env);
    copy.currentTime = currentTime;
    copy.threshold = threshold;
    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.addNamedOption(TTL, "time to live (milliseconds)");
    io.addNamedOption(CURRENT_TIME, "if set, use the given value as the absolute time in milliseconds as the current time of day");
    io.setName("ageoff");
    io.setDescription("AgeOffFilter removes entries with timestamps more than <ttl> milliseconds old");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (super.validateOptions(options) == false)
      return false;
    try {
      Long.parseLong(options.get(TTL));
    } catch (Exception e) {
      throw new IllegalArgumentException("bad long " + TTL + ":" + options.get(TTL));
    }
    return true;
  }

  /**
   * A convenience method for setting the age off threshold.
   *
   * @param is
   *          IteratorSetting object to configure.
   * @param ttl
   *          age off threshold in milliseconds.
   */
  public static void setTTL(IteratorSetting is, Long ttl) {
    is.addOption(TTL, Long.toString(ttl));
  }

  /**
   * A convenience method for setting the current time (from which to measure the age off threshold).
   *
   * @param is
   *          IteratorSetting object to configure.
   * @param currentTime
   *          time in milliseconds.
   */
  public static void setCurrentTime(IteratorSetting is, Long currentTime) {
    is.addOption(CURRENT_TIME, Long.toString(currentTime));
  }
}
