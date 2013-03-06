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
package org.apache.accumulo.core.util.shell;

public class ShellCommandException extends Exception {
  private static final long serialVersionUID = 1L;

  public enum ErrorCode {
    UNKNOWN_ERROR("Unknown error"),
    UNSUPPORTED_LANGUAGE("Programming language used is not supported"),
    UNRECOGNIZED_COMMAND("Command is not supported"),
    INITIALIZATION_FAILURE("Command could not be initialized"),
    XML_PARSING_ERROR("Failed to parse the XML file");

    private String description;

    private ErrorCode(String description) {
      this.description = description;
    }

    public String getDescription() {
      return this.description;
    }

    public String toString() {
      return getDescription();
    }
  }

  private ErrorCode code;
  private String command;

  public ShellCommandException(ErrorCode code) {
    this(code, null);
  }

  public ShellCommandException(ErrorCode code, String command) {
    this.code = code;
    this.command = command;
  }

  public String getMessage() {
    return code + (command != null ? " (" + command + ")" : "");
  }
}
