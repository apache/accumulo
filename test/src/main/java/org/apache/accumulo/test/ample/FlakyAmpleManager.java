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
package org.apache.accumulo.test.ample;

import java.io.IOException;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.manager.Manager;

public class FlakyAmpleManager extends Manager {

  protected FlakyAmpleManager(ConfigOpts opts, String[] args) throws IOException {
    super(opts, FlakyAmpleServerContext::new, args);
  }

  public static void main(String[] args) throws Exception {
    try (FlakyAmpleManager manager = new FlakyAmpleManager(new ConfigOpts(), args)) {
      manager.runServer();
    }
  }
}
