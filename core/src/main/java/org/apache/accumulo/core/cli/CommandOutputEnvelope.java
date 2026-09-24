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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.google.gson.Gson;

/**
 * A stable, versioned outer wrapper for all admin command JSON output.
 *
 * <p>
 * Every command that supports --json output wraps its command-specific data in this envelope. This
 * provides a consistent structure that scripts can rely on regardless of which command produced the
 * output:
 *
 * <pre>
 * {
 *   "command": "accumulo admin fate --summary",
 *   "version": "1",
 *   "reportTime": "2026-06-04T12:00:00Z",
 *   "status": "OK"
 *   },
 *   "output": { ... command-specific payload... }
 * }
 * </pre>
 *
 * <p>
 * The {@link CommandStatus#version} field is a stability contract. When a breaking change is made
 * to the envelope structure, the version will be incremented. Scripts should check this field and
 * handle the version they were written against.
 *
 */
public class CommandOutputEnvelope {

  /**
   * Current envelop schema version. Increment this if a breaking structural change is made to the
   * envelope fields (not to the {@code output} field, data changes command specific).
   */
  public static final String VERSION = "1.0";
  private static final DateTimeFormatter ISO_FMT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
  private static final Gson PRETTY_GSON =
      new Gson().newBuilder().setPrettyPrinting().disableJdkUnsafe().create();

  public static class CommandStatus {
    private String command;
    private String version;
    private String reportTime;
    private String statusMessage;

    @SuppressWarnings("unused")
    private CommandStatus() {}

    private CommandStatus(String command, String statusMessage) {
      this.command = command;
      this.version = VERSION;
      this.reportTime = ISO_FMT.format(ZonedDateTime.now(ZoneId.systemDefault()));
      this.statusMessage = statusMessage;
    }

    public String getCommand() {
      return command;
    }

    public String getVersion() {
      return version;
    }

    public String getReportTime() {
      return reportTime;
    }

    public String getStatusMessage() {
      return statusMessage;
    }
  }

  private CommandStatus status;
  private Object output;

  @SuppressWarnings("unused")
  private CommandOutputEnvelope() {}

  private CommandOutputEnvelope(String command, String statusMessage, Object output) {
    this.status = new CommandStatus(command, statusMessage);
    this.output = output;
  }

  public static CommandOutputEnvelope of(String command, Object data) {
    return new CommandOutputEnvelope(command, "OK", data);
  }

  public static CommandOutputEnvelope error(String command, String message) {
    return new CommandOutputEnvelope(command, "ERROR" + message, null);
  }

  public String toJson() {
    return PRETTY_GSON.toJson(this);
  }

  public static CommandOutputEnvelope fromJson(String json) {
    return PRETTY_GSON.fromJson(json, CommandOutputEnvelope.class);
  }

  public CommandStatus getStatus() {
    return status;
  }

  public Object getOutput() {
    return output;
  }
}
