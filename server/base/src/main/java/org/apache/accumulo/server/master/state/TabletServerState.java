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
package org.apache.accumulo.server.master.state;

import java.util.HashMap;
import java.util.HashSet;

public enum TabletServerState {
  // not a valid state, reserved for internal use only
  RESERVED((byte) (-1)),

  // the following are normally functioning states
  NEW((byte) 0),
  ONLINE((byte) 1),
  UNRESPONSIVE((byte) 2),
  DOWN((byte) 3),

  // the following are bad states and cause tservers to be ignored by the master
  BAD_SYSTEM_PASSWORD((byte) 101),
  BAD_VERSION((byte) 102),
  BAD_INSTANCE((byte) 103),
  BAD_CONFIG((byte) 104),
  BAD_VERSION_AND_INSTANCE((byte) 105),
  BAD_VERSION_AND_CONFIG((byte) 106),
  BAD_VERSION_AND_INSTANCE_AND_CONFIG((byte) 107),
  BAD_INSTANCE_AND_CONFIG((byte) 108);

  private byte id;

  private static HashMap<Byte,TabletServerState> mapping;
  private static HashSet<TabletServerState> badStates;

  static {
    mapping = new HashMap<>(TabletServerState.values().length);
    badStates = new HashSet<>();
    for (TabletServerState state : TabletServerState.values()) {
      mapping.put(state.id, state);
      if (state.id > 99)
        badStates.add(state);
    }
  }

  private TabletServerState(byte id) {
    this.id = id;
  }

  public byte getId() {
    return this.id;
  }

  public static TabletServerState getStateById(byte id) {
    if (mapping.containsKey(id))
      return mapping.get(id);
    throw new IndexOutOfBoundsException("No such state");
  }

}
