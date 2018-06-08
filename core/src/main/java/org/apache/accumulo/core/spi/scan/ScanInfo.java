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
package org.apache.accumulo.core.spi.scan;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.hadoop.io.Text;

/**
 * @since 2.0.0
 */
public interface ScanInfo {

  enum Type {
    SINGLE, MULTI
  }

  public interface Stats {
    long min();

    long max();

    double mean();

    long sum();

    long num();
  }

  Type getScanType();

  // String getTableName(); //TODO

  String getTableId();

  int getNumTablets();

  int getNumRanges();

  long getCreationTime();

  // TODO maybe avoid using optional in API because of object creation???
  OptionalLong getLastRunTime();

  Optional<Stats> getRunTimeStats();

  Optional<Stats> getIdleTimeStats();

  Stats getIdleTimeStats(long currentTime);

  Collection<Text> getFetchedFamilies();

  List<IteratorSetting> getScanIterators();

  public static long getCurrentTime() {
    return System.currentTimeMillis();
  }
}
