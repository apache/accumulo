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

package org.apache.accumulo.core.client.admin;

import java.util.Collections;
import java.util.Map;

public class TableSummary {
  public enum Status {
    /**
     * Requested summary was computed for all data
     */
    PRESENT,

    /**
     * Requested summary were computed for some data.
     */
    PARTIALLY_PRESENT,

    /**
     * Requested summary was not seen in any data.
     */
    NOT_PRESENT
  }

  public Status getStatus() {
    return Status.NOT_PRESENT;
  }

  public String getId() {
    return "";
  }

  /**
   * @return the name of the class that generated this summary
   */
  public String getSummarizer() {
    return "";
  }

  public Map<String,Long> getSummary() {
    return Collections.emptyMap();
  }
}
