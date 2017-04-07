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
package org.apache.accumulo.core.client.admin;

import java.util.SortedSet;

public class DiskUsage {

  protected final SortedSet<String> tables;
  protected Long usage;

  public DiskUsage(SortedSet<String> tables, Long usage) {
    this.tables = tables;
    this.usage = usage;
  }

  public SortedSet<String> getTables() {
    return tables;
  }

  public Long getUsage() {
    return usage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof DiskUsage))
      return false;

    DiskUsage diskUsage = (DiskUsage) o;

    if (tables != null ? !tables.equals(diskUsage.tables) : diskUsage.tables != null)
      return false;
    if (usage != null ? !usage.equals(diskUsage.usage) : diskUsage.usage != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = tables != null ? tables.hashCode() : 0;
    result = 31 * result + (usage != null ? usage.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DiskUsage{" + "tables=" + tables + ", usage=" + usage + '}';
  }
}
