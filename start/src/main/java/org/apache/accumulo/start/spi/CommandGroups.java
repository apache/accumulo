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

public class CommandGroups {

  public static final CommandGroup ADMIN = new AdminCommandGroup();
  public static final CommandGroup CLIENT = new ClientCommandGroup();
  public static final CommandGroup COMPACTION = new CompactionCommandGroup();
  public static final CommandGroup CORE = new CoreCommandGroup();
  public static final CommandGroup OTHER = new OtherCommandGroup();
  public static final CommandGroup PROCESS = new ProcessCommandGroup();

  public static abstract class BaseCommandGroup implements CommandGroup {

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
      if (obj instanceof CommandGroup) {
        return this.compareTo((CommandGroup) obj) == 0;
      }
      return false;
    }

  }

  public static class AdminCommandGroup extends BaseCommandGroup {

    private AdminCommandGroup() {}

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

  public static class ClientCommandGroup extends BaseCommandGroup {

    private ClientCommandGroup() {}

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

  public static class CompactionCommandGroup extends BaseCommandGroup {

    private CompactionCommandGroup() {}

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

  public static class CoreCommandGroup extends BaseCommandGroup {

    private CoreCommandGroup() {}

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

  public static class ProcessCommandGroup extends BaseCommandGroup {

    private ProcessCommandGroup() {}

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

  public static class OtherCommandGroup extends BaseCommandGroup {

    private OtherCommandGroup() {}

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
