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
package org.apache.accumulo.monitor.util;

/**
 * Simple utility class to validate Accumulo Monitor Query and Path parameters
 */
public interface ParameterValidator {

  String ALPHA_NUM_REGEX = "\\w+";
  String ALPHA_NUM_REGEX_BLANK_OK = "\\w*";

  String RESOURCE_REGEX = "(\\w|:)+";

  String NAMESPACE_REGEX = "[*-]?|(\\w)+";
  String NAMESPACE_LIST_REGEX = "[*-]?|(\\w+,?\\w*)+";

  String SERVER_REGEX = "(\\w+([.-])*\\w*)+(:[0-9]+)*";
  String SERVER_REGEX_BLANK_OK = "((\\w+([.-])*\\w*)+(:[0-9]+)*)*";
}
