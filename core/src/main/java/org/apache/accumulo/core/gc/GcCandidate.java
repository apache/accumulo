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
package org.apache.accumulo.core.gc;

import java.lang.Object;
import java.util.Objects;

import org.apache.accumulo.core.metadata.StoredTabletFile;

public class GcCandidate implements Comparable<GcCandidate> {
  private final Long uid;
  private final String path;

  public GcCandidate(String path, Long uid) {
    this.path = path;
    this.uid = uid;
  }

  public String getPath() {
    return path;
  }

  public Long getUid() {
    return uid;
  }

  public String getFileName() {
    return new StoredTabletFile(path).getFileName();
  }

  public String getParent() {
    return new StoredTabletFile(path).getPath().getParent().toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, uid);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof GcCandidate) {
      GcCandidate candidate = (GcCandidate) obj;
      return this.uid.equals(candidate.getUid()) && this.path.equals(candidate.getPath());
    }
    return false;
  }

  @Override
  public int compareTo(GcCandidate candidate) {
    var cmp = this.path.compareTo(candidate.getPath());
    if (cmp == 0) {
      return this.uid.compareTo(candidate.getUid());
    } else {
      return cmp;
    }
  }

  @Override
  public String toString() {
    return path + ", UUID: " + uid;
  }
}
