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

/**
 * Used for grouping {@link KeywordExecutable} commands.
 *
 * @since 4.0.0
 */
public interface CommandGroup extends Comparable<CommandGroup> {

  /**
   * To execute a command in this group this key must be entered before the command. Should not
   * contain spaces. Evaluation at runtime is case-insensitive. So if the keyword is {@code Foo} and
   * the user types {@code foo} it will still work.
   */
  String key();

  /**
   * The title is informational and is used when displaying all commands in this group.
   */
  String title();

  /**
   * The description is informational and provides more details about a group.
   */
  String description();

  /**
   * Compares on {@link #key()}, then {@link #title()}, and then {@link #description()}.
   */
  @Override
  default int compareTo(CommandGroup o) {
    int result = this.key().compareTo(o.key());
    if (result == 0) {
      result = this.title().compareTo(o.title());
      if (result == 0) {
        result = this.description().compareTo(o.description());
      }
    }
    return result;
  }

}
