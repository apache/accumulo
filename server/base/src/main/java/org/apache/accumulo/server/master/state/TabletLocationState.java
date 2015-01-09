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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.hadoop.io.Text;

/**
 * When a tablet is assigned, we mark its future location. When the tablet is opened, we set its current location. A tablet should never have both a future and
 * current location.
 *
 * A tablet server is always associated with a unique session id. If the current tablet server has a different session, we know the location information is
 * out-of-date.
 */
public class TabletLocationState {

  static public class BadLocationStateException extends Exception {
    private static final long serialVersionUID = 1L;
    private Text metadataTableEntry;

    BadLocationStateException(String msg, Text row) {
      super(msg);
      this.metadataTableEntry = row;
    }

    public Text getEncodedEndRow() {
      return metadataTableEntry;
    }
  }

  public TabletLocationState(KeyExtent extent, TServerInstance future, TServerInstance current, TServerInstance last, Collection<Collection<String>> walogs,
      boolean chopped) throws BadLocationStateException {
    this.extent = extent;
    this.future = future;
    this.current = current;
    this.last = last;
    if (walogs == null)
      walogs = Collections.emptyList();
    this.walogs = walogs;
    this.chopped = chopped;
    if (current != null && future != null) {
      throw new BadLocationStateException(extent + " is both assigned and hosted, which should never happen: " + this, extent.getMetadataEntry());
    }
  }

  final public KeyExtent extent;
  final public TServerInstance future;
  final public TServerInstance current;
  final public TServerInstance last;
  final public Collection<Collection<String>> walogs;
  final public boolean chopped;

  public String toString() {
    return extent + "@(" + future + "," + current + "," + last + ")" + (chopped ? " chopped" : "");
  }

  public TServerInstance getServer() {
    TServerInstance result = null;
    if (current != null) {
      result = current;
    } else if (future != null) {
      result = future;
    } else {
      result = last;
    }
    return result;
  }

  public TabletState getState(Set<TServerInstance> liveServers) {
    TServerInstance server = getServer();
    if (server == null)
      return TabletState.UNASSIGNED;
    if (server.equals(current) || server.equals(future)) {
      if (liveServers.contains(server))
        if (server.equals(future)) {
          return TabletState.ASSIGNED;
        } else {
          return TabletState.HOSTED;
        }
      else {
        return TabletState.ASSIGNED_TO_DEAD_SERVER;
      }
    }
    // server == last
    return TabletState.UNASSIGNED;
  }

}
