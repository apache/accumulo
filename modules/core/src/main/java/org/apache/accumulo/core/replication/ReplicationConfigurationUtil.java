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
package org.apache.accumulo.core.replication;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;

/**
 * Encapsulates configuration semantics around replication
 */
public class ReplicationConfigurationUtil {

  /**
   * Determines if the replication is enabled for the given {@link KeyExtent}
   *
   * @param extent
   *          The {@link KeyExtent} for the Tablet in question
   * @param conf
   *          The {@link AccumuloConfiguration} for that Tablet (table or namespace)
   * @return True if this extent is a candidate for replication at the given point in time.
   */
  public static boolean isEnabled(KeyExtent extent, AccumuloConfiguration conf) {
    if (extent.isMeta() || extent.isRootTablet()) {
      return false;
    }

    return conf.getBoolean(Property.TABLE_REPLICATION);
  }

}
