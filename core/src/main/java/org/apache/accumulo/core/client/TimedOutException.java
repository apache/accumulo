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
package org.apache.accumulo.core.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

/**
 * @since 1.5.0
 */
public class TimedOutException extends RuntimeException {

  private Set<String> timedoutServers;

  private static final long serialVersionUID = 1L;

  private static String shorten(Set<String> set) {
    if (set.size() < 10) {
      return set.toString();
    }

    return new ArrayList<String>(set).subList(0, 10).toString() + " ... " + (set.size() - 10) + " servers not shown";
  }

  public TimedOutException(Set<String> timedoutServers) {
    super("Servers timed out " + shorten(timedoutServers));
    this.timedoutServers = timedoutServers;

  }

  public TimedOutException(String msg) {
    super(msg);
    this.timedoutServers = Collections.emptySet();
  }

  public Set<String> getTimedOutSevers() {
    return Collections.unmodifiableSet(timedoutServers);
  }
}
