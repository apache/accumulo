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

package org.apache.accumulo.core.client.summary;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * This is an example showing that really efficient summarizers can be written. There are no map lookups per key value.
 * 
 * <p>
 * Also shows how it eassily something besides counting is to do, like min and max.
 */
public class ExampleSummarizer implements KeyValueSummarizer {
  private long minStamp = Long.MAX_VALUE;
  private long maxStamp = Long.MIN_VALUE;
  private long deletes = 0;
  private long total = 0;

  @Override
  public String getId() {
    return "accumulo.example123";
  }

  @Override
  public void collect(Key k, Value v) {
    if (k.getTimestamp() < minStamp) {
      minStamp = k.getTimestamp();
    }

    if (k.getTimestamp() > maxStamp) {
      maxStamp = k.getTimestamp();
    }

    if (k.isDeleted()) {
      deletes++;
    }

    total++;
  }

  @Override
  public Map<String,Long> summarize() {
    HashMap<String,Long> ret = new HashMap<>();

    ret.put("minStamp", minStamp);
    ret.put("maxStamp", maxStamp);
    ret.put("deletes", deletes);
    ret.put("total", total);

    return ret;
  }

  @Override
  public void merge(Map<String,Long> summary1, Map<String,Long> summary2) {
    summary1.merge("deletes", summary2.getOrDefault("deletes", 0l), Long::sum);
    summary1.merge("total", summary2.getOrDefault("total", 0l), Long::sum);
    summary1.merge("minStamp", summary2.getOrDefault("minStamp", Long.MAX_VALUE), Long::min);
    summary1.merge("maxStamp", summary2.getOrDefault("maxStamp", Long.MIN_VALUE), Long::max);
  }
}
