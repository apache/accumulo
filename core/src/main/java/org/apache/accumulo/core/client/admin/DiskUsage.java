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
package org.apache.accumulo.core.client.admin;

import java.util.Objects;
import java.util.SortedSet;

public class DiskUsage {

  protected final SortedSet<String> tables;
  protected long usage;

  public DiskUsage(SortedSet<String> tables, long usage) {
    this.tables = tables;
    this.usage = usage;
  }

  public SortedSet<String> getTables() {
    return tables;
  }

  public long getUsage() {
    return usage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DiskUsage)) {
      return false;
    }

    DiskUsage diskUsage = (DiskUsage) o;

    return Objects.equals(tables, diskUsage.tables) && Objects.equals(usage, diskUsage.usage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tables, usage);
  }

  @Override
  public String toString() {
    return "DiskUsage{tables=" + tables + ", usage=" + usage + '}';
  }
}
