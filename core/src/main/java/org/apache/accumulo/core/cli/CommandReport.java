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
package org.apache.accumulo.core.cli;

import java.util.List;

/**
 * Implemented by all command report classes that support both human-readable and computer readable
 * outputs.
 * <p>
 * Json output is always wrapped in a {@link CommandOutputEnvelope} to provide a stable, versioned
 * outer structure that scripts can depend on, regardless of which command is used.
 *
 * <p>
 * Usage pattern is a command's execute() method:
 *
 * <pre>
 * CommandReport report = buildReport(context, options);
 * if (options.json()) {
 *   System.out.println(report.toEnvelopedJson("accumulo admin <my-command>"));
 * } else {
 *   report.formatLines().forEach(System.out::println);
 * }
 * </pre>
 */
public interface CommandReport {
  List<String> formatLines();

  Object getData();

  default String toEnvelopedJson(String commandName) {
    return CommandOutputEnvelope.of(commandName, getData()).toJson();
  }

}
