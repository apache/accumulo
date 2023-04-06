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
package org.apache.accumulo.core.metadata.schema;

import java.util.UUID;

import org.apache.accumulo.core.data.AbstractId;

public class ExternalCompactionId extends AbstractId<ExternalCompactionId> {

  // A common prefix is nice when grepping logs for external compaction ids. The prefix also serves
  // as a nice sanity check on data coming in over the network and from persistent storage.
  private static final String PREFIX = "ECID:";

  private ExternalCompactionId(UUID uuid) {
    super(PREFIX + uuid);
  }

  private ExternalCompactionId(String id) {
    super(id);
  }

  private static final long serialVersionUID = 1L;

  public static ExternalCompactionId generate(UUID uuid) {
    return new ExternalCompactionId(uuid);
  }

  public static ExternalCompactionId of(String id) {
    if (!id.startsWith(PREFIX)) {
      throw new IllegalArgumentException("Not a valid external compaction id " + id);
    }

    try {
      UUID.fromString(id.substring(PREFIX.length()));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Not a valid external compaction id " + id, e);
    }

    return new ExternalCompactionId(id);
  }

  /**
   * Sanitize user input for the ECID string with proper "ECID:" prefix.
   */
  public static ExternalCompactionId from(String ecid) {
    ecid = ecid.replace(PREFIX.toLowerCase(), PREFIX);
    if (!ecid.startsWith(PREFIX)) {
      ecid = PREFIX + ecid;
    }
    return of(ecid);
  }

}
