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
package org.apache.accumulo.core.metadata;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

/**
 * When a tablet is assigned, we mark its future location. When the tablet is opened, we set its
 * current location. A tablet should never have both a future and current location.
 *
 * A tablet server is always associated with a unique session id. If the current tablet server has a
 * different session, we know the location information is out-of-date.
 */
public class TabletLocationState {

  public static class BadLocationStateException extends Exception {
    private static final long serialVersionUID = 2L;

    // store as byte array because Text isn't Serializable
    private final byte[] metadataTableEntry;

    public BadLocationStateException(String msg, Text row) {
      super(msg);
      this.metadataTableEntry = TextUtil.getBytes(requireNonNull(row));
    }

    public Text getEncodedEndRow() {
      return new Text(metadataTableEntry);
    }
  }

  public TabletLocationState(KeyExtent extent, TServerInstance future, TServerInstance current,
      TServerInstance last, SuspendingTServer suspend, Collection<Collection<String>> walogs,
      boolean chopped) throws BadLocationStateException {
    this.extent = extent;
    this.future = future;
    this.current = current;
    this.last = last;
    this.suspend = suspend;
    if (walogs == null) {
      walogs = Collections.emptyList();
    }
    this.walogs = walogs;
    this.chopped = chopped;
    if (hasCurrent() && hasFuture()) {
      throw new BadLocationStateException(
          extent + " is both assigned and hosted, which should never happen: " + this,
          extent.toMetaRow());
    }
  }

  public final KeyExtent extent;
  public final TServerInstance future;
  public final TServerInstance current;
  public final TServerInstance last;
  public final SuspendingTServer suspend;
  public final Collection<Collection<String>> walogs;
  public final boolean chopped;

  public TServerInstance futureOrCurrent() {
    if (hasCurrent()) {
      return current;
    }
    return future;
  }

  @Override
  public String toString() {
    return extent + "@(" + future + "," + current + "," + last + ")" + (chopped ? " chopped" : "");
  }

  public TServerInstance getLocation() {
    TServerInstance result = null;
    if (hasCurrent()) {
      result = current;
    } else if (hasFuture()) {
      result = future;
    } else {
      result = last;
    }
    return result;
  }

  public boolean hasCurrent() {
    return current != null;
  }

  public boolean hasFuture() {
    return future != null;
  }

  public boolean hasSuspend() {
    return suspend != null;
  }

  public TabletState getState(Set<TServerInstance> liveServers) {
    if (hasFuture()) {
      return liveServers.contains(future) ? TabletState.ASSIGNED
          : TabletState.ASSIGNED_TO_DEAD_SERVER;
    } else if (hasCurrent()) {
      return liveServers.contains(current) ? TabletState.HOSTED
          : TabletState.ASSIGNED_TO_DEAD_SERVER;
    } else if (hasSuspend()) {
      return TabletState.SUSPENDED;
    } else {
      return TabletState.UNASSIGNED;
    }
  }
}
