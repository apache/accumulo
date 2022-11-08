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
package org.apache.accumulo.master;

import org.apache.accumulo.manager.MasterExecutable;
import org.apache.accumulo.start.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated since 2.1.0. Use {@link Main} with keyword "manager" instead.
 */
@Deprecated(since = "2.1.0")
public class Master {
  private static final Logger LOG = LoggerFactory.getLogger(Master.class);

  public static void main(String[] args) throws Exception {
    LOG.warn("Usage of {} directly has been deprecated. Use {} with keyword manager instead.",
        Master.class.getName(), Main.class.getName());
    new MasterExecutable().execute(args);
  }
}
