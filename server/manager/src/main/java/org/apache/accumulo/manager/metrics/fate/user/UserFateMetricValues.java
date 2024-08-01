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
package org.apache.accumulo.manager.metrics.fate.user;

import java.util.Map;

import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.manager.metrics.fate.FateMetricValues;
import org.apache.accumulo.manager.metrics.fate.FateMetrics;

import com.google.common.base.Preconditions;

public class UserFateMetricValues extends FateMetricValues {

  protected UserFateMetricValues(long updateTime, long currentFateOps,
      Map<String,Long> txStateCounters, Map<String,Long> opTypeCounters) {
    super(updateTime, currentFateOps, txStateCounters, opTypeCounters);
  }

  public static UserFateMetricValues getUserStoreMetrics(
      final ReadOnlyFateStore<FateMetrics<UserFateMetricValues>> userFateStore) {
    Preconditions.checkArgument(userFateStore.type() == FateInstanceType.USER,
        "Fate store must be of type %s", FateInstanceType.USER);
    Builder builder = getFateMetrics(userFateStore, UserFateMetricValues.builder());
    return builder.build();
  }

  @Override
  public String toString() {
    return "MetaFateMetricValues{updateTime=" + updateTime + ", currentFateOps=" + currentFateOps
        + '}';
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder extends AbstractBuilder<Builder,UserFateMetricValues> {

    @Override
    protected UserFateMetricValues build() {
      return new UserFateMetricValues(System.currentTimeMillis(), currentFateOps, txStateCounters,
          opTypeCounters);
    }
  }
}
