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
package org.apache.accumulo.monitor.util;

/**
 * Simple utility class to validate Accumulo Monitor Query and Path parameters
 */
public interface ParameterValidator {

  String ALPHA_NUM_REGEX = "\\w+";
  // Allow the special default table IDs
  String ALPHA_NUM_REGEX_TABLE_ID = "[!+]?\\w+";
  String ALPHA_NUM_REGEX_BLANK_OK = "\\w*";
  String PROBLEM_TYPE_REGEX = "FILE_READ|FILE_WRITE|TABLET_LOAD";
  String RESOURCE_REGEX = "(?:)(.*)";
  // host name and port
  String HOSTNAME_PORT_REGEX = "[a-zA-Z0-9.-]+:[0-9]{2,5}";
}
