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
package org.apache.accumulo.start.spi;

import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;

import com.google.auto.service.AutoService;

public class UsageGroups {

  public static final UsageGroup ADMIN = new AdminUsageGroup();
  public static final UsageGroup CLIENT = new ClientUsageGroup();
  public static final UsageGroup COMPACTION = new CompactionUsageGroup();
  public static final UsageGroup CORE = new CoreUsageGroup();
  public static final UsageGroup OTHER = new OtherUsageGroup();
  public static final UsageGroup PROCESS = new ProcessUsageGroup();

  private static Set<UsageGroup> groups = null;

  public static synchronized Set<UsageGroup> getUsageGroups() {
    if (groups == null) {
      Set<UsageGroup> ug = new TreeSet<>();
      ServiceLoader.load(UsageGroup.class).forEach(ug::add);
      groups = ug;
    }
    return groups;
  }

  public static abstract class BaseUsageGroup implements UsageGroup {

    @Override
    public int hashCode() {
      return Objects.hash(key(), title(), description());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (obj instanceof UsageGroup) {
        return this.compareTo((UsageGroup) obj) == 0;
      }
      return false;
    }

  }

  @AutoService(UsageGroup.class)
  public static class AdminUsageGroup extends BaseUsageGroup {

    @Override
    public String key() {
      return "admin";
    }

    @Override
    public String title() {
      return "Admin";
    }

    @Override
    public String description() {
      return "Administrative commands.";
    }
  }

  @AutoService(UsageGroup.class)
  public static class ClientUsageGroup extends BaseUsageGroup {

    @Override
    public String key() {
      return "";
    }

    @Override
    public String title() {
      return "Client";
    }

    @Override
    public String description() {
      return "Client commands, requires accumulo-client.properties only, group is optional in command.";
    }
  }

  @AutoService(UsageGroup.class)
  public static class CompactionUsageGroup extends BaseUsageGroup {

    @Override
    public String key() {
      return "compaction";
    }

    @Override
    public String title() {
      return "Compaction";
    }

    @Override
    public String description() {
      return "Compaction related commands";
    }
  }

  @AutoService(UsageGroup.class)
  public static class CoreUsageGroup extends BaseUsageGroup {

    @Override
    public String key() {
      return "core";
    }

    @Override
    public String title() {
      return "Core";
    }

    @Override
    public String description() {
      return "Core commands";
    }
  }

  @AutoService(UsageGroup.class)
  public static class ProcessUsageGroup extends BaseUsageGroup {

    @Override
    public String key() {
      return "process";
    }

    @Override
    public String title() {
      return "Process";
    }

    @Override
    public String description() {
      return "Process related commands";
    }
  }

  @AutoService(UsageGroup.class)
  public static class OtherUsageGroup extends BaseUsageGroup {

    @Override
    public String key() {
      return "other";
    }

    @Override
    public String title() {
      return "Other";
    }

    @Override
    public String description() {
      return "Other commands";
    }
  }

}
