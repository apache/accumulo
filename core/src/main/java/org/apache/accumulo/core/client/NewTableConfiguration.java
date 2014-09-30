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
package org.apache.accumulo.core.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.iterators.IteratorUtil;

/**
 * This object stores table creation parameters. Currently including: TimeType, limitVersion, and user specified initial properties
 * 
 * @since 1.7.0
 */
public class NewTableConfiguration {

  private static final TimeType DEFAULT_TIME_TYPE = TimeType.MILLIS;
  private TimeType timeType = DEFAULT_TIME_TYPE;

  private boolean limitVersion = true;

  private Map<String,String> properties = new HashMap<String,String>();

  public NewTableConfiguration setTimeType(TimeType tt) {
    checkArgument(tt != null, "TimeType is null");

    this.timeType = tt;
    return this;
  }

  public TimeType getTimeType() {
    return timeType;
  }

  /**
   * Currently the only default iterator is the versioning iterator. This method will cause the table to be created without the versioning iterator
   */
  public NewTableConfiguration withoutDefaultIterators() {
    this.limitVersion = false;
    return this;
  }

  public NewTableConfiguration setProperties(Map<String,String> prop) {
    checkArgument(prop != null, "properties is null");

    this.properties = new HashMap<String,String>(prop);
    return this;
  }

  public Map<String,String> getProperties() {
    Map<String,String> propertyMap = new HashMap<>();

    if (limitVersion) {
      propertyMap.putAll(IteratorUtil.generateInitialTableProperties(limitVersion));
    }

    propertyMap.putAll(properties);
    return Collections.unmodifiableMap(propertyMap);
  }
}
