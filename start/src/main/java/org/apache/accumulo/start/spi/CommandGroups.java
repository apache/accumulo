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

/**
 * @since 4.0.0
 */
public class CommandGroups {

  public static final CommandGroup CLIENT = new ClientCommandGroup();
  public static final CommandGroup COMPACTION = new CompactionCommandGroup();
  public static final CommandGroup CONFIG = new ConfigCommandGroup();
  public static final CommandGroup FILE = new FileCommandGroup();
  public static final CommandGroup INSTANCE = new InstanceCommandGroup();
  public static final CommandGroup OTHER = new OtherCommandGroup();
  public static final CommandGroup PROCESS = new ProcessCommandGroup();
  public static final CommandGroup TABLE = new TableCommandGroup();

  /**
   * @since 4.0.0
   */
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

  /**
   * @since 4.0.0
   */
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
      return "Client commands";
    }
  }

  /**
   * @since 4.0.0
   */
  public static class CompactionCommandGroup extends BaseCommandGroup {

    private CompactionCommandGroup() {}

    @Override
    public String key() {
      return "compact";
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

  /**
   * @since 4.0.0
   */
  public static class ConfigCommandGroup extends BaseCommandGroup {

    private ConfigCommandGroup() {}

    @Override
    public String key() {
      return "conf";
    }

    @Override
    public String title() {
      return "Configuration";
    }

    @Override
    public String description() {
      return "Configuration related commands";
    }
  }

  /**
   * @since 4.0.0
   */
  public static class FileCommandGroup extends BaseCommandGroup {

    private FileCommandGroup() {}

    @Override
    public String key() {
      return "file";
    }

    @Override
    public String title() {
      return "File";
    }

    @Override
    public String description() {
      return "File related commands";
    }
  }

  /**
   * @since 4.0.0
   */
  public static class InstanceCommandGroup extends BaseCommandGroup {

    private InstanceCommandGroup() {}

    @Override
    public String key() {
      return "inst";
    }

    @Override
    public String title() {
      return "Instance";
    }

    @Override
    public String description() {
      return "Instance related commands";
    }
  }

  /**
   * @since 4.0.0
   */
  public static class ProcessCommandGroup extends BaseCommandGroup {

    private ProcessCommandGroup() {}

    @Override
    public String key() {
      return "proc";
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

  /**
   * @since 4.0.0
   */
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

  /**
   * @since 4.0.0
   */
  public static class TableCommandGroup extends BaseCommandGroup {

    private TableCommandGroup() {}

    @Override
    public String key() {
      return "table";
    }

    @Override
    public String title() {
      return "Table";
    }

    @Override
    public String description() {
      return "Table related commands";
    }
  }

}
