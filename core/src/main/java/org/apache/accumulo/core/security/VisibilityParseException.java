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
package org.apache.accumulo.core.security;

import static com.google.common.base.Charsets.UTF_8;

import java.text.ParseException;

/**
 * An exception thrown when a visibility string cannot be parsed.
 */
public class VisibilityParseException extends ParseException {
  private static final long serialVersionUID = 1L;
  private String visibility;

  /**
   * Creates a new exception.
   *
   * @param reason
   *          reason string
   * @param visibility
   *          visibility that could not be parsed
   * @param errorOffset
   *          offset into visibility where parsing failed
   */
  public VisibilityParseException(String reason, byte[] visibility, int errorOffset) {
    super(reason, errorOffset);
    this.visibility = new String(visibility, UTF_8);
  }

  @Override
  public String getMessage() {
    return super.getMessage() + " in string '" + visibility + "' at position " + super.getErrorOffset();
  }
}
