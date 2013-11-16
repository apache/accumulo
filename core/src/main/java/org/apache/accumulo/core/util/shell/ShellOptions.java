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

import org.apache.commons.cli.Option;

/**
 * Abstract class to encompass the Options available on the Accumulo Shell
 */
public abstract class ShellOptions {
  // Global options flags
  public static final String userOption = "u";
  public static final String tableOption = "t";
  public static final String namespaceOption = "ns";
  public static final String helpOption = "?";
  public static final String helpLongOption = "help";

  final Option helpOpt = new Option(helpOption, helpLongOption, false, "display this help");
}
