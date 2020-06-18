/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.metrics;

import org.apache.accumulo.server.metrics.Metrics;

abstract class TServerMetrics extends Metrics {

  protected static final String TSERVER_NAME = "TabletServer";

  protected TServerMetrics(String record) {
    // this capitalization thing is just to preserve the capitalization difference between the
    // "general" record and the "TabletServer,sub=General" metrics name that existed in 1.9 without
    // duplicating too much code; the description, however, did change between 1.9 and 2.0
    super("TabletServer,sub=" + capitalize(record),
        "TabletServer " + capitalize(record) + " Metrics", "tserver", record);
  }

  private static final String capitalize(String word) {
    return word.substring(0, 1).toUpperCase() + word.substring(1);
  }

}
